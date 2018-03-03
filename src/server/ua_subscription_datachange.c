/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. 
 *
 *    Copyright 2017 (c) Julius Pfrommer, Fraunhofer IOSB
 *    Copyright 2017 (c) Stefan Profanter, fortiss GmbH
 *    Copyright 2018 (c) Thomas Stalder, Blue Time Concept SA
 */

#include "ua_subscription.h"
#include "ua_server_internal.h"
#include "ua_types_encoding_binary.h"

#ifdef UA_ENABLE_SUBSCRIPTIONS /* conditional compilation */

#define UA_VALUENCODING_MAXSTACK 512

UA_MonitoredItem *
UA_MonitoredItem_new(UA_MonitoredItemType monType) {
    /* Allocate the memory */
    UA_MonitoredItem *newItem =
            (UA_MonitoredItem *) UA_calloc(1, sizeof(UA_MonitoredItem));
    if(!newItem)
        return NULL;

    /* Remaining members are covered by calloc zeroing out the memory */
    newItem->monitoredItemType = monType; /* currently hardcoded */
    newItem->timestampsToReturn = UA_TIMESTAMPSTORETURN_SOURCE;
    TAILQ_INIT(&newItem->queue);
    return newItem;
}

void
MonitoredItem_delete(UA_Server *server, UA_MonitoredItem *monitoredItem) {
    UA_Subscription *sub = monitoredItem->subscription;
    UA_LOG_WARNING_SESSION(server->config.logger, sub->session,
                           "Subscription %u | MonitoredItem %i | "
                           "Delete the MonitoredItem",
                           sub->subscriptionId, monitoredItem->monitoredItemId);

    if(monitoredItem->monitoredItemType == UA_MONITOREDITEMTYPE_CHANGENOTIFY) {
        /* Remove the sampling callback */
        MonitoredItem_unregisterSampleCallback(server, monitoredItem);

        /* Clear the queued samples */
        UA_Notification *notification, *notification_tmp;
        TAILQ_FOREACH_SAFE(notification, &monitoredItem->queue, listEntry, notification_tmp) {
            TAILQ_REMOVE(&monitoredItem->queue, notification, listEntry);

            /* Remove the item in the global queue */
            TAILQ_REMOVE(&sub->notificationQueue, notification, globalEntry);
            UA_DataValue_deleteMembers(&notification->data.value);
            UA_free(notification);

            if (sub->pendingNotifications)
                sub->pendingNotifications--;
            else
                sub->readyNotifications--;
        }
        monitoredItem->currentQueueSize = 0;
    } else {
        /* TODO: Access val data.event */
        UA_LOG_ERROR(server->config.logger, UA_LOGCATEGORY_SERVER,
                     "MonitoredItemTypes other than ChangeNotify are not supported yet");
        return;
    }
    /* Remove the monitored item */
    LIST_REMOVE(monitoredItem, listEntry);
    UA_String_deleteMembers(&monitoredItem->indexRange);
    UA_ByteString_deleteMembers(&monitoredItem->lastSampledValue);
    UA_NodeId_deleteMembers(&monitoredItem->monitoredNodeId);
    UA_Server_delayedFree(server, monitoredItem);
}

void
MonitoredItem_ensureQueueSpace(UA_Subscription *sub, UA_MonitoredItem *mon,
                               UA_Notification *newNotification) {
    UA_Boolean valueDiscarded = false;
    UA_Notification *notification;
#ifndef __clang_analyzer__
    while(mon->currentQueueSize > mon->maxQueueSize) {
        /* maxQueuesize is at least 1 */
        UA_assert(mon->currentQueueSize >= 2);

        /* Get the item to remove. New items are added to the end */
        if(mon->discardOldest) {
            /* Remove the oldest */
            notification = TAILQ_FIRST(&mon->queue);
        } else {
            /* Keep the newest, remove the second-newest */
            notification = TAILQ_LAST(&mon->queue, NotificationQueue);
            notification = TAILQ_PREV(notification, NotificationQueue, listEntry);
        }
        UA_assert(notification);

        /* Remove the item */
        TAILQ_REMOVE(&mon->queue, notification, listEntry);
        if(mon->monitoredItemType == UA_MONITOREDITEMTYPE_CHANGENOTIFY) {
            UA_DataValue_deleteMembers(&notification->data.value);
        } else {
            //TODO: event implemantation
        }

        UA_Notification *nextGlobalNotification = TAILQ_NEXT(notification, globalEntry);
        TAILQ_REMOVE(&sub->notificationQueue, notification, globalEntry);

        if(newNotification) {
            if(nextGlobalNotification)
                TAILQ_INSERT_BEFORE(nextGlobalNotification, newNotification, globalEntry);
            else
                TAILQ_INSERT_TAIL(&sub->notificationQueue, newNotification, globalEntry);
            newNotification = NULL;
        } else { 
            if (sub->pendingNotifications)
                --sub->pendingNotifications;
            else
                --sub->readyNotifications;
        }

        UA_free(notification);
        --mon->currentQueueSize;
        valueDiscarded = true;
    }
#endif

    if(!valueDiscarded)
        goto end;

    if(mon->monitoredItemType == UA_MONITOREDITEMTYPE_CHANGENOTIFY) {
        /* Get the element that carries the infobits */
        if(mon->discardOldest)
            notification = TAILQ_FIRST(&mon->queue);
        else
            notification = TAILQ_LAST(&mon->queue, NotificationQueue);
        UA_assert(notification);

        /* If the queue size is reduced to one, remove the infobits */
        if(mon->maxQueueSize == 1) {
            notification->data.value.status &= ~(UA_StatusCode) (UA_STATUSCODE_INFOTYPE_DATAVALUE |
                                                              UA_STATUSCODE_INFOBITS_OVERFLOW);

            goto end;
        }

        /* Add the infobits either to the newest or the new last entry */
        notification->data.value.hasStatus = true;
        notification->data.value.status |= (UA_STATUSCODE_INFOTYPE_DATAVALUE | UA_STATUSCODE_INFOBITS_OVERFLOW);
    }
end:
    if(newNotification) {
        TAILQ_INSERT_TAIL(&sub->notificationQueue, newNotification, globalEntry);
        ++sub->pendingNotifications;
    }
}

/* Errors are returned as no change detected */
static UA_Boolean
detectValueChangeWithFilter(UA_MonitoredItem *mon, UA_DataValue *value,
                            UA_ByteString *encoding) {
    /* Encode the data for comparison */
    size_t binsize = UA_calcSizeBinary(value, &UA_TYPES[UA_TYPES_DATAVALUE]);
    if(binsize == 0)
        return false;

    /* Allocate buffer on the heap if necessary */
    if(binsize > UA_VALUENCODING_MAXSTACK &&
       UA_ByteString_allocBuffer(encoding, binsize) != UA_STATUSCODE_GOOD)
        return false;

    /* Encode the value */
    UA_Byte *bufPos = encoding->data;
    const UA_Byte *bufEnd = &encoding->data[encoding->length];
    UA_StatusCode retval = UA_encodeBinary(value, &UA_TYPES[UA_TYPES_DATAVALUE],
                                           &bufPos, &bufEnd, NULL, NULL);
    if(retval != UA_STATUSCODE_GOOD)
        return false;

    /* The value has changed */
    encoding->length = (uintptr_t)bufPos - (uintptr_t)encoding->data;
    return !mon->lastSampledValue.data || !UA_String_equal(encoding, &mon->lastSampledValue);
}

/* Has this sample changed from the last one? The method may allocate additional
 * space for the encoding buffer. Detect the change in encoding->data. */
static UA_Boolean
detectValueChange(UA_MonitoredItem *mon, UA_DataValue *value, UA_ByteString *encoding) {
    /* Apply Filter */
    UA_Boolean hasValue = value->hasValue;
    if(mon->trigger == UA_DATACHANGETRIGGER_STATUS)
        value->hasValue = false;

    UA_Boolean hasServerTimestamp = value->hasServerTimestamp;
    UA_Boolean hasServerPicoseconds = value->hasServerPicoseconds;
    value->hasServerTimestamp = false;
    value->hasServerPicoseconds = false;

    UA_Boolean hasSourceTimestamp = value->hasSourceTimestamp;
    UA_Boolean hasSourcePicoseconds = value->hasSourcePicoseconds;
    if(mon->trigger < UA_DATACHANGETRIGGER_STATUSVALUETIMESTAMP) {
        value->hasSourceTimestamp = false;
        value->hasSourcePicoseconds = false;
    }

    /* Detect the Value Change */
    UA_Boolean res = detectValueChangeWithFilter(mon, value, encoding);

    /* Reset the filter */
    value->hasValue = hasValue;
    value->hasServerTimestamp = hasServerTimestamp;
    value->hasServerPicoseconds = hasServerPicoseconds;
    value->hasSourceTimestamp = hasSourceTimestamp;
    value->hasSourcePicoseconds = hasSourcePicoseconds;
    return res;
}

/* Returns whether a new sample was created */
static UA_Boolean
sampleCallbackWithValue(UA_Server *server, UA_Subscription *sub,
                        UA_MonitoredItem *monitoredItem,
                        UA_DataValue *value,
                        UA_ByteString *valueEncoding) {
    UA_assert(monitoredItem->monitoredItemType == UA_MONITOREDITEMTYPE_CHANGENOTIFY);
    /* Store the pointer to the stack-allocated bytestring to see if a heap-allocation
     * was necessary */
    UA_Byte *stackValueEncoding = valueEncoding->data;

    /* Has the value changed? */
    UA_Boolean changed = detectValueChange(monitoredItem, value, valueEncoding);
    if(!changed)
        return false;

    /* Allocate the entry for the publish queue */
    UA_Notification *newNotification =
        (UA_Notification *)UA_malloc(sizeof(UA_Notification));
    if(!newNotification) {
        UA_LOG_WARNING_SESSION(server->config.logger, sub->session,
                               "Subscription %u | MonitoredItem %i | "
                               "Item for the publishing queue could not be allocated",
                               sub->subscriptionId, monitoredItem->monitoredItemId);
        return false;
    }

    /* Copy valueEncoding on the heap for the next comparison (if not already done) */
    if(valueEncoding->data == stackValueEncoding) {
        UA_ByteString cbs;
        if(UA_ByteString_copy(valueEncoding, &cbs) != UA_STATUSCODE_GOOD) {
            UA_LOG_WARNING_SESSION(server->config.logger, sub->session,
                                   "Subscription %u | MonitoredItem %i | "
                                   "ByteString to compare values could not be created",
                                   sub->subscriptionId, monitoredItem->monitoredItemId);
            UA_free(newNotification);
            return false;
        }
        *valueEncoding = cbs;
    }

    /* Prepare the newQueueItem */
    if(value->hasValue && value->value.storageType == UA_VARIANT_DATA_NODELETE) {
        /* Make a deep copy of the value */
        UA_StatusCode retval = UA_DataValue_copy(value, &newNotification->data.value);
        if(retval != UA_STATUSCODE_GOOD) {
            UA_LOG_WARNING_SESSION(server->config.logger, sub->session,
                                   "Subscription %u | MonitoredItem %i | "
                                   "Item for the publishing queue could not be prepared",
                                   sub->subscriptionId, monitoredItem->monitoredItemId);
            UA_free(newNotification);
            return false;
        }
    } else {
        newNotification->data.value = *value; /* Just copy the value and do not release it */
    }

    /* <-- Point of no return --> */

    UA_LOG_DEBUG_SESSION(server->config.logger, sub->session,
                         "Subscription %u | MonitoredItem %u | Sampled a new value",
                         sub->subscriptionId, monitoredItem->monitoredItemId);

    newNotification->mon = monitoredItem;

    /* Replace the encoding for comparison */
    UA_ByteString_deleteMembers(&monitoredItem->lastSampledValue);
    monitoredItem->lastSampledValue = *valueEncoding;

    /* Add the sample to the queue for publication */
    TAILQ_INSERT_TAIL(&monitoredItem->queue, newNotification, listEntry);
    ++monitoredItem->currentQueueSize;

    /* Remove entries from the queue if required and add the sample to the global queue */
    MonitoredItem_ensureQueueSpace(sub, monitoredItem, newNotification);

    return true;
}

void
UA_MonitoredItem_SampleCallback(UA_Server *server,
                                UA_MonitoredItem *monitoredItem) {
    UA_Subscription *sub = monitoredItem->subscription;
    if(monitoredItem->monitoredItemType != UA_MONITOREDITEMTYPE_CHANGENOTIFY) {
        UA_LOG_DEBUG_SESSION(server->config.logger, sub->session,
                             "Subscription %u | MonitoredItem %i | "
                             "Not a data change notification",
                             sub->subscriptionId, monitoredItem->monitoredItemId);
        return;
    }

    /* Read the value */
    UA_ReadValueId rvid;
    UA_ReadValueId_init(&rvid);
    rvid.nodeId = monitoredItem->monitoredNodeId;
    rvid.attributeId = monitoredItem->attributeId;
    rvid.indexRange = monitoredItem->indexRange;
    UA_DataValue value =
        UA_Server_readWithSession(server, sub->session,
                                  &rvid, monitoredItem->timestampsToReturn);

    /* Stack-allocate some memory for the value encoding. We might heap-allocate
     * more memory if needed. This is just enough for scalars and small
     * structures. */
    UA_Byte *stackValueEncoding = (UA_Byte *)UA_alloca(UA_VALUENCODING_MAXSTACK);
    UA_ByteString valueEncoding;
    valueEncoding.data = stackValueEncoding;
    valueEncoding.length = UA_VALUENCODING_MAXSTACK;

    /* Create a sample and compare with the last value */
    UA_Boolean newNotification = sampleCallbackWithValue(server, sub, monitoredItem,
                                                         &value, &valueEncoding);

    /* Clean up */
    if(!newNotification) {
        if(valueEncoding.data != stackValueEncoding)
            UA_ByteString_deleteMembers(&valueEncoding);
        UA_DataValue_deleteMembers(&value);
    }
}

UA_StatusCode
MonitoredItem_registerSampleCallback(UA_Server *server, UA_MonitoredItem *mon) {
    if(mon->sampleCallbackIsRegistered)
        return UA_STATUSCODE_GOOD;
    UA_StatusCode retval =
        UA_Server_addRepeatedCallback(server, (UA_ServerCallback)UA_MonitoredItem_SampleCallback,
                                      mon, (UA_UInt32)mon->samplingInterval, &mon->sampleCallbackId);
    if(retval == UA_STATUSCODE_GOOD)
        mon->sampleCallbackIsRegistered = true;
    return retval;
}

UA_StatusCode
MonitoredItem_unregisterSampleCallback(UA_Server *server, UA_MonitoredItem *mon) {
    if(!mon->sampleCallbackIsRegistered)
        return UA_STATUSCODE_GOOD;
    mon->sampleCallbackIsRegistered = false;
    return UA_Server_removeRepeatedCallback(server, mon->sampleCallbackId);
}

#endif /* UA_ENABLE_SUBSCRIPTIONS */
