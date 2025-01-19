/********************************************************************************
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use async_trait::async_trait;
use core::hash::{Hash, Hasher};
#[cfg(test)]
use mockall::automock;

pub use crate::up_core_api::usubscription::{
    fetch_subscriptions_request::Request, subscription_status::State, EventDeliveryConfig,
    FetchSubscribersRequest, FetchSubscribersResponse, FetchSubscriptionsRequest,
    FetchSubscriptionsResponse, NotificationsRequest, NotificationsResponse, SubscribeAttributes,
    SubscriberInfo, Subscription, SubscriptionRequest, SubscriptionResponse, SubscriptionStatus,
    UnsubscribeRequest, UnsubscribeResponse, Update,
};

use crate::{UStatus, UUri};

// Tracks information for the SubscriptionCache
pub struct SubscriptionInformation {
    pub topic: UUri,
    pub subscriber: SubscriberInfo,
    pub status: SubscriptionStatus,
    pub attributes: SubscribeAttributes,
    pub config: EventDeliveryConfig,
}

impl Eq for SubscriptionInformation {}

impl PartialEq for SubscriptionInformation {
    fn eq(&self, other: &Self) -> bool {
        self.subscriber == other.subscriber
    }
}

impl Hash for SubscriptionInformation {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.subscriber.hash(state);
    }
}

impl Clone for SubscriptionInformation {
    fn clone(&self) -> Self {
        Self {
            topic: self.topic.clone(),
            subscriber: self.subscriber.clone(),
            status: self.status.clone(),
            attributes: self.attributes.clone(),
            config: self.config.clone(),
        }
    }
}

impl Hash for SubscriberInfo {
    /// Creates a hash value based on the URI property.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use std::hash::{DefaultHasher, Hash, Hasher};
    /// use up_rust::UUri;
    /// use up_rust::core::usubscription::SubscriberInfo;
    ///
    /// let mut hasher = DefaultHasher::new();
    /// let info = SubscriberInfo {
    ///     uri: Some(UUri::try_from_parts("", 0x1000, 0x01, 0x9a00).unwrap()).into(),
    ///     ..Default::default()
    /// };
    ///
    /// info.hash(&mut hasher);
    /// let hash_one = hasher.finish();
    ///
    /// let mut hasher = DefaultHasher::new();
    /// let info = SubscriberInfo {
    ///     uri: Some(UUri::try_from_parts("", 0x1000, 0x02, 0xf100).unwrap()).into(),
    ///     ..Default::default()
    /// };
    ///
    /// info.hash(&mut hasher);
    /// let hash_two = hasher.finish();
    ///
    /// assert_ne!(hash_one, hash_two);
    /// ```
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.uri.hash(state);
    }
}

impl Eq for SubscriberInfo {}

/// Checks if a given [`SubscriberInfo`] contains any information.
///
/// # Returns
///
/// `true` if the given instance is equal to [`SubscriberInfo::default`], `false` otherwise.
///
/// # Examples
///
/// ```rust
/// use up_rust::UUri;
/// use up_rust::core::usubscription::SubscriberInfo;
///
/// let mut info = SubscriberInfo::default();
/// assert!(info.is_empty());
///
/// info.uri = Some(UUri::try_from_parts("", 0x1000, 0x01, 0x9a00).unwrap()).into();
/// assert!(!info.is_empty());
/// ```
impl SubscriberInfo {
    pub fn is_empty(&self) -> bool {
        self.eq(&SubscriberInfo::default())
    }
}

impl SubscriptionResponse {
    /// Checks if this `SubscriptionResponse` is in a specific state (`usubscription::State``).
    ///
    /// Returns `true` if SubscriptionReponse contains a valied SusbcriptionStatus, which has a
    /// state property that is equal to state passed as argument.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use up_rust::core::usubscription::{SubscriptionResponse, SubscriptionStatus, State};
    ///
    /// let subscription_response = SubscriptionResponse {
    ///     status: Some(SubscriptionStatus {
    ///         state: State::SUBSCRIBED.into(),
    ///         ..Default::default()
    ///         }).into(),
    ///     ..Default::default()
    /// };
    /// assert!(subscription_response.is_state(State::SUBSCRIBED));
    /// ```
    pub fn is_state(&self, state: State) -> bool {
        self.status
            .as_ref()
            .is_some_and(|ss| ss.state.enum_value().is_ok_and(|s| s.eq(&state)))
    }
}

/// The uEntity (type) identifier of the uSubscription service.
pub const USUBSCRIPTION_TYPE_ID: u32 = 0x0000_0000;
/// The (latest) major version of the uSubscription service.
pub const USUBSCRIPTION_VERSION_MAJOR: u8 = 0x03;
/// The resource identifier of uSubscription's _subscribe_ operation.
pub const RESOURCE_ID_SUBSCRIBE: u16 = 0x0001;
/// The resource identifier of uSubscription's _unsubscribe_ operation.
pub const RESOURCE_ID_UNSUBSCRIBE: u16 = 0x0002;
/// The resource identifier of uSubscription's _fetch subscriptions_ operation.
pub const RESOURCE_ID_FETCH_SUBSCRIPTIONS: u16 = 0x0003;
/// The resource identifier of uSubscription's _register for notifications_ operation.
pub const RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS: u16 = 0x0006;
/// The resource identifier of uSubscription's _unregister for notifications_ operation.
pub const RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS: u16 = 0x0007;
/// The resource identifier of uSubscription's _fetch subscribers_ operation.
pub const RESOURCE_ID_FETCH_SUBSCRIBERS: u16 = 0x0008;

/// The resource identifier of uSubscription's _subscription change_ topic.
pub const RESOURCE_ID_SUBSCRIPTION_CHANGE: u16 = 0x8000;

/// Gets a UUri referring to one of the local uSubscription service's resources.
///
/// # Examples
///
/// ```rust
/// use up_rust::core::usubscription;
///
/// let uuri = usubscription::usubscription_uri(usubscription::RESOURCE_ID_SUBSCRIBE);
/// assert_eq!(uuri.resource_id, 0x0001);
/// ```
pub fn usubscription_uri(resource_id: u16) -> UUri {
    UUri::try_from_parts(
        "",
        USUBSCRIPTION_TYPE_ID,
        USUBSCRIPTION_VERSION_MAJOR,
        resource_id,
    )
    .unwrap()
}

/// The uProtocol Application Layer client interface to the uSubscription service.
///
/// Please refer to the [uSubscription service specification](https://github.com/eclipse-uprotocol/up-spec/blob/main/up-l3/usubscription/v3/README.adoc)
/// for details.
#[cfg_attr(test, automock)]
#[async_trait]
pub trait USubscription: Send + Sync {
    /// Subscribe to a topic, using a [`SubscriptionRequest`]
    ///
    /// # Parameters
    ///
    /// * `subscription_request` - A request to subscribe
    ///
    /// # Returns
    ///
    /// * [`SubscriptionResponse`] detailing if subscription was successful with other metadata
    async fn subscribe(
        &self,
        subscription_request: SubscriptionRequest,
    ) -> Result<SubscriptionResponse, UStatus>;

    /// Unsubscribe to a topic, using an [`UnsubscribeRequest`]
    ///
    /// # Parameters
    ///
    /// * `unsubscribe_request` - A request to unsubscribe
    ///
    /// # Returns
    ///
    /// * [`UStatus`] detailing if unsubscription was successful and if not why not
    async fn unsubscribe(&self, unsubscribe_request: UnsubscribeRequest) -> Result<(), UStatus>;

    /// Fetch all subscriptions for a given topic or subscriber contained inside a [`FetchSubscriptionsRequest`]
    ///
    /// # Parameters
    ///
    /// * `fetch_subscriptions_request` - A request to fetch subscriptions given a topic or subscriber
    ///
    /// # Returns
    ///
    /// * [`FetchSubscriptionsResponse`] detailing the zero or more subscriptions' info
    async fn fetch_subscriptions(
        &self,
        fetch_subscriptions_request: FetchSubscriptionsRequest,
    ) -> Result<FetchSubscriptionsResponse, UStatus>;

    /// Register for notifications relevant to a given topic inside a [`NotificationsRequest`]
    /// changing in subscription status.
    ///
    /// # Parameters
    ///
    /// * `notifications_register_request` - A request to receive changes to subscription status
    ///
    /// # Returns
    ///
    /// * [`UStatus`] detailing if notification registration was successful and if not why not
    async fn register_for_notifications(
        &self,
        notifications_register_request: NotificationsRequest,
    ) -> Result<(), UStatus>;

    /// Unregister for notifications relevant to a given topic inside a [`NotificationsRequest`]
    /// changing in subscription status.
    ///
    /// # Parameters
    ///
    /// * `notifications_unregister_request` - A request to no longer receive changes to subscription status
    ///
    /// # Returns
    ///
    /// * [`UStatus`] detailing if notification unregistration was successful and if not why not
    async fn unregister_for_notifications(
        &self,
        notifications_unregister_request: NotificationsRequest,
    ) -> Result<(), UStatus>;

    /// Fetch a list of subscribers that are currently subscribed to a given topic in a [`FetchSubscribersRequest`]
    ///
    /// # Parameters
    ///
    /// * `fetch_subscribers_request` - Request containing topic for which we'd like all subscribers' info
    ///
    /// # Returns
    ///
    /// * [`FetchSubscriptionsResponse`] detailing subscriber info for the provided topic
    async fn fetch_subscribers(
        &self,
        fetch_subscribers_request: FetchSubscribersRequest,
    ) -> Result<FetchSubscribersResponse, UStatus>;
}
