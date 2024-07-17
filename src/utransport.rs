/********************************************************************************
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
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

use std::fmt::{Debug, Formatter};
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

use async_trait::async_trait;
#[cfg(test)]
use mockall::automock;

use crate::{UCode, UMessage, UStatus, UUri};

/// A factory for URIs representing this uEntity's resources.
///
/// Implementations may use arbitrary mechanisms to determine the information that
/// is necessary for creating URIs, e.g. environment variables, configuration files etc.
#[cfg_attr(test, automock)]
pub trait LocalUriProvider: Send + Sync {
    /// Gets the _authority_ used for URIs representing this uEntity's resources.
    fn get_authority(&self) -> String;
    /// Gets a URI that represents a given resource of this uEntity.
    fn get_resource_uri(&self, resource_id: u16) -> UUri;
    /// Gets the URI that represents the resource that this uEntity expects
    /// RPC responses and notifications to be sent to.
    fn get_source_uri(&self) -> UUri;
}

/// A handler for processing uProtocol messages.
///
/// Implementations of `UListener` contain the details for what should occur when a message is received
///
/// For more information, please refer to [uProtocol Specification](https://github.com/eclipse-uprotocol/up-spec/blob/main/up-l1/README.adoc).
///
/// # Examples
///
/// ## Simple example
///
/// ```
/// use up_rust::{UListener, UMessage, UStatus};
///
/// use async_trait::async_trait;
/// use std::sync::{Arc, Mutex};
///
/// #[derive(Clone)]
/// struct FooListener {
///     inner_foo: Arc<Mutex<String>>
/// }
///
/// #[async_trait]
/// impl UListener for FooListener {
///     async fn on_receive(&self, msg: UMessage) {
///         let mut inner_foo = self.inner_foo.lock().unwrap();
///         if let Some(payload) = msg.payload.as_ref() {
///             *inner_foo = format!("latest message length: {}", payload.len());
///         }
///     }
/// }
/// ```
///
/// ## Long-running function needed when message received
///
/// ```
/// use up_rust::{UListener, UMessage, UStatus};
///
/// use async_trait::async_trait;
/// use tokio::task;
/// use std::sync::{Arc, Mutex};
///
/// #[derive(Clone)]
/// struct LongTaskListener;
///
/// async fn send_to_jupiter(message_for_jupiter: UMessage) {
///     // send a message to Jupiter
///     println!("Fly me to the moon... {message_for_jupiter}");
/// }
///
/// #[async_trait]
/// impl UListener for LongTaskListener {
///     async fn on_receive(&self, msg: UMessage) {
///         tokio::spawn(send_to_jupiter(msg));
///     }
/// }
/// ```
#[cfg_attr(test, automock)]
#[async_trait]
pub trait UListener: Send + Sync {
    /// Performs some action on receipt of a message.
    ///
    /// # Parameters
    ///
    /// * `msg` - The message
    ///
    /// # Note for `UListener` implementers
    ///
    /// `on_receive()` is expected to return almost immediately. If it does not, it could potentially
    /// block further message receipt. For long-running operations consider passing off received
    /// data to a different async function to handle it and returning.
    ///
    /// # Note for `UTransport` implementers
    ///
    /// Because `on_receive()` is async you may choose to either `.await` it in the current context
    /// or spawn it onto a new task and await there to allow current context to immediately continue.
    async fn on_receive(&self, msg: UMessage);
}

/// [`UTransport`] is the uP-L1 interface that provides a common API for uE developers to send and receive messages.
///
/// Implementations of [`UTransport`] contain the details for connecting to the underlying transport technology and
/// sending [`UMessage`][crate::UMessage] using the configured technology. For more information, please refer to
/// [uProtocol Specification](https://github.com/eclipse-uprotocol/up-spec/blob/main/up-l1/README.adoc).
// [impl->req~up-language-transport-api~1]
#[async_trait]
pub trait UTransport: Send + Sync {
    /// Sends a message using this transport's message exchange mechanism.
    ///
    /// # Arguments
    ///
    /// * `message` - The message to send. The `type`, `source` and`sink` properties of the [`crate::UAttributes`] contained
    ///   in the message determine the addressing semantics:
    ///   * `source` - The origin of the message being sent. The address must be resolved. The semantics of the address
    ///     depends on the value of the given [attributes' type](crate::UAttributes::type_) property .
    ///     * For a [`PUBLISH`](crate::UMessageType::UMESSAGE_TYPE_PUBLISH) message, this is the topic that the message should be published to,
    ///     * for a [`REQUEST`](crate::UMessageType::UMESSAGE_TYPE_REQUEST) message, this is the *reply-to* address that the sender expects to receive the response at, and
    ///     * for a [`RESPONSE`](crate::UMessageType::UMESSAGE_TYPE_RESPONSE) message, this identifies the method that has been invoked.
    ///   * `sink` - For a `notification`, an RPC `request` or RPC `response` message, the (resolved) address that the message
    ///     should be sent to.
    ///
    /// # Errors
    ///
    /// Returns an error if the message could not be sent.
    async fn send(&self, message: UMessage) -> Result<(), UStatus>;

    /// Receives a message from the transport.
    ///
    /// This default implementation returns an error with [`UCode::UNIMPLEMENTED`].
    ///
    /// # Arguments
    ///
    /// * `source_filter` - The [source](`crate::UAttributes::source`) address pattern that the message to receive needs to match.
    /// * `sink_filter` - The [sink](`crate::UAttributes::sink`) address pattern that the message to receive needs to match,
    ///                   or `None` to indicate that the message must not contain any sink address.
    ///
    /// # Errors
    ///
    /// Returns an error if no message could be received, e.g. because no message matches the given addresses.
    async fn receive(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
    ) -> Result<UMessage, UStatus> {
        Err(UStatus::fail_with_code(
            UCode::UNIMPLEMENTED,
            "not implemented",
        ))
    }

    /// Registers a listener to be called for messages.
    ///
    /// The listener will be invoked for each message that matches the given source and sink filter patterns
    /// according to the rules defined by the [UUri specification](https://github.com/eclipse-uprotocol/up-spec/blob/main/basics/uri.adoc).
    ///
    /// This default implementation returns an error with [`UCode::UNIMPLEMENTED`].
    ///
    /// # Arguments
    ///
    /// * `source_filter` - The [source](`crate::UAttributes::source`) address pattern that messages need to match.
    /// * `sink_filter` - The [sink](`crate::UAttributes::sink`) address pattern that messages need to match,
    ///                   or `None` to match messages that do not contain any sink address.
    /// * `listener` - The listener to invoke.
    ///                The listener can be unregistered again using [`UTransport::unregister_listener`].
    ///
    /// # Errors
    ///
    /// Returns an error if the listener could not be registered.
    ///
    /// # Examples
    ///
    /// ## Registering a listener
    ///
    /// ```
    /// use std::sync::Arc;
    /// use up_rust::UListener;
    /// # use up_rust::{UMessage, UStatus, UTransport, UUri};
    /// # use async_trait::async_trait;
    /// #
    /// # pub struct MyTransport;
    /// #
    /// # impl MyTransport {
    /// #     pub fn new()->Self {
    /// #         Self
    /// #     }
    /// # }
    /// #
    /// # #[async_trait]
    /// # impl UTransport for MyTransport {
    /// #     async fn send(&self, _message: UMessage) -> Result<(), UStatus> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn receive(&self, _source: &UUri, _sink: Option<&UUri>) -> Result<UMessage, UStatus> {
    /// #         todo!()
    /// #     }
    /// #
    /// #     async fn register_listener(&self, _source: &UUri, _sink: Option<&UUri>, _listener: Arc<dyn UListener>) -> Result<(), UStatus> {
    /// #         Ok(())
    /// #     }
    /// #
    /// #     async fn unregister_listener(&self, _source: &UUri, _sink: Option<&UUri>, _listener: Arc<dyn UListener>) -> Result<(), UStatus> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// #
    /// # #[derive(Default)]
    /// # pub struct MyListener;
    /// #
    /// # #[async_trait]
    /// # impl UListener for MyListener {
    /// #     async fn on_receive(&self, msg: UMessage) {
    /// #         todo!()
    /// #     }
    /// # }
    /// #
    /// # let mut my_transport = MyTransport::new();
    /// # let source_uuri = UUri::default();
    ///
    /// // hang onto this listener...
    /// let my_listener = Arc::new(MyListener::default());
    /// // ...use a clone for registering...
    /// my_transport.register_listener(
    ///     &source_uuri,
    ///     None,
    ///     my_listener.clone());
    /// // ...and use the original we hung onto when unregistering
    /// my_transport.unregister_listener(
    ///     &source_uuri,
    ///     None,
    ///     my_listener);
    /// ```
    async fn register_listener(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
        _listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        Err(UStatus::fail_with_code(
            UCode::UNIMPLEMENTED,
            "not implemented",
        ))
    }

    /// Unregisters a message listener.
    ///
    /// The listener will no longer be called for any (matching) messages after this function has
    /// returned successfully.
    ///
    /// This default implementation returns an error with [`UCode::UNIMPLEMENTED`].
    ///
    /// # Arguments
    ///
    /// * `source_filter` - The source address pattern that the listener had been registered for.
    /// * `sink_filter` - The sink address pattern that the listener had been registered for.
    /// * `listener` - The listener to unregister.
    ///
    /// # Errors
    ///
    /// Returns an error if the listener could not be unregistered, for example if the given listener does not exist.
    async fn unregister_listener(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
        _listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        Err(UStatus::fail_with_code(
            UCode::UNIMPLEMENTED,
            "not implemented",
        ))
    }
}

#[cfg(test)]
mockall::mock! {
    /// This extra struct is necessary in order to comply with mockall's requirements regarding the parameter lifetimes
    /// see https://github.com/asomers/mockall/issues/571
    pub Transport {
        pub async fn do_send(&self, message: UMessage) -> Result<(), UStatus>;
        pub async fn do_register_listener<'a>(&'a self, source_filter: &'a UUri, sink_filter: Option<&'a UUri>, listener: Arc<dyn UListener>) -> Result<(), UStatus>;
        pub async fn do_unregister_listener<'a>(&'a self, source_filter: &'a UUri, sink_filter: Option<&'a UUri>, listener: Arc<dyn UListener>) -> Result<(), UStatus>;
    }
}

#[cfg(test)]
#[async_trait]
/// This delegates the invocation of the UTransport functions to the mocked functions of the Transport struct.
/// see https://github.com/asomers/mockall/issues/571
impl UTransport for MockTransport {
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        self.do_send(message).await
    }
    async fn register_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        self.do_register_listener(source_filter, sink_filter, listener)
            .await
    }
    async fn unregister_listener(
        &self,
        source_filter: &UUri,
        sink_filter: Option<&UUri>,
        listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        self.do_unregister_listener(source_filter, sink_filter, listener)
            .await
    }
}

/// A wrapper type that allows comparing [`UListener`]s to each other.
///
/// # Note
///
/// Not necessary for end-user uEs to use. Primarily intended for `up-client-foo-rust` UPClient libraries
/// when implementing [`UTransport`].
///
/// # Rationale
///
/// The wrapper type is implemented such that it can be used in any location you may wish to
/// hold a type implementing [`UListener`].
///
/// Implements necessary traits to allow hashing, so that you may hold the wrapper type in
/// collections which require that, such as a `HashMap` or `HashSet`
#[derive(Clone)]
pub struct ComparableListener {
    listener: Arc<dyn UListener>,
}

impl ComparableListener {
    pub fn new(listener: Arc<dyn UListener>) -> Self {
        Self { listener }
    }
    /// Gets a clone of the wrapped reference to the listener.
    pub fn into_inner(&self) -> Arc<dyn UListener> {
        self.listener.clone()
    }

    /// Allows us to get the pointer address of this `ComparableListener` on the heap
    fn pointer_address(&self) -> usize {
        // Obtain the raw pointer from the Arc
        let ptr = Arc::as_ptr(&self.listener);
        // Cast the fat pointer to a raw thin pointer to ()
        let thin_ptr = ptr as *const ();
        // Convert the thin pointer to a usize
        thin_ptr as usize
    }
}

impl Deref for ComparableListener {
    type Target = dyn UListener;

    fn deref(&self) -> &Self::Target {
        &*self.listener
    }
}

impl Hash for ComparableListener {
    /// Feeds the pointer to the listener held by `self` into the given [`Hasher`].
    ///
    /// This is consistent with the implementation of [`ComparableListener::eq`].
    fn hash<H: Hasher>(&self, state: &mut H) {
        Arc::as_ptr(&self.listener).hash(state);
    }
}

impl PartialEq for ComparableListener {
    /// Compares this listener to another listener.
    ///
    /// # Returns
    ///
    /// `true` if the pointer to the listener held by `self` is equal to the pointer held by `other`.
    /// This is consistent with the implementation of [`ComparableListener::hash`].
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.listener, &other.listener)
    }
}

impl Eq for ComparableListener {}

impl Debug for ComparableListener {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "ComparableListener: {}", self.pointer_address())
    }
}

#[cfg(test)]
mod tests {
    use crate::ComparableListener;
    use crate::UListener;
    use crate::{UCode, UMessage, UStatus, UTransport, UUri};
    use async_trait::async_trait;
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::sync::Mutex;
    use std::thread;

    #[derive(Default)]
    struct UPClientFoo {
        source_listeners: Mutex<HashMap<UUri, HashSet<ComparableListener>>>,
    }

    impl UPClientFoo {
        fn check_on_receive(&mut self, source: &UUri, umessage: &UMessage) -> Result<(), UStatus> {
            self.source_listeners
                .lock()
                .unwrap()
                .get(source)
                .ok_or_else(|| {
                    UStatus::fail_with_code(
                        UCode::NOT_FOUND,
                        format!("No listeners registered for source address: {:?}", &source),
                    )
                })
                .and_then(|listeners| {
                    if listeners.is_empty() {
                        Err(UStatus::fail_with_code(
                            UCode::NOT_FOUND,
                            format!("No listeners registered for topic: {:?}", &source),
                        ))
                    } else {
                        for listener in listeners.iter() {
                            let task_listener = listener.clone();
                            let task_umessage = umessage.clone();
                            tokio::spawn(
                                async move { task_listener.on_receive(task_umessage).await },
                            );
                        }
                        Ok(())
                    }
                })
        }
    }

    #[async_trait]
    impl UTransport for UPClientFoo {
        async fn send(&self, _message: UMessage) -> Result<(), UStatus> {
            todo!()
        }

        async fn receive(
            &self,
            _source_filter: &UUri,
            _sink_filter: Option<&UUri>,
        ) -> Result<UMessage, UStatus> {
            todo!()
        }

        async fn register_listener(
            &self,
            source_filter: &UUri,
            _sink_filter: Option<&UUri>,
            listener: Arc<dyn UListener>,
        ) -> Result<(), UStatus> {
            let identified_listener = ComparableListener::new(listener);
            if self
                .source_listeners
                .lock()
                .unwrap()
                .entry(source_filter.to_owned())
                .or_default()
                .insert(identified_listener)
            {
                Ok(())
            } else {
                Err(UStatus::fail_with_code(
                    UCode::ALREADY_EXISTS,
                    "UUri + UListener pair already exists!",
                ))
            }
        }

        async fn unregister_listener(
            &self,
            source_filter: &UUri,
            _sink_filter: Option<&UUri>,
            listener: Arc<dyn UListener>,
        ) -> Result<(), UStatus> {
            self.source_listeners
                .lock()
                .unwrap()
                .get_mut(source_filter)
                .ok_or_else(|| {
                    UStatus::fail_with_code(
                        UCode::NOT_FOUND,
                        format!(
                            "No listeners registered for source address: {:?}",
                            source_filter
                        ),
                    )
                })
                .and_then(|listeners| {
                    let identified_listener = ComparableListener::new(listener);
                    if listeners.remove(&identified_listener) {
                        Ok(())
                    } else {
                        Err(UStatus::fail_with_code(
                            UCode::NOT_FOUND,
                            "UUri + UListener not found!",
                        ))
                    }
                })
        }
    }

    #[derive(Clone, Debug)]
    struct ListenerBaz;
    #[async_trait]
    impl UListener for ListenerBaz {
        async fn on_receive(&self, msg: UMessage) {
            println!("Printing msg from ListenerBaz! received: {:?}", msg);
        }
    }

    #[derive(Clone, Debug)]
    struct ListenerBar;
    #[async_trait]
    impl UListener for ListenerBar {
        async fn on_receive(&self, msg: UMessage) {
            println!("Printing msg from ListenerBar! received: {:?}", msg);
        }
    }

    fn uuri_factory(uuri_index: u8) -> UUri {
        match uuri_index {
            1 => UUri {
                authority_name: "uuri_1".to_string(),
                ..Default::default()
            },
            _ => UUri::default(),
        }
    }

    #[tokio::test]
    async fn test_register_and_receive() {
        let mut up_client_foo = UPClientFoo::default();
        let uuri_1 = uuri_factory(1);
        let listener_baz: Arc<dyn UListener> = Arc::new(ListenerBaz);
        let register_res = up_client_foo
            .register_listener(&uuri_1, None, listener_baz.clone())
            .await;
        assert!(register_res.is_ok());

        let umessage = UMessage::default();
        let check_on_receive_res = up_client_foo.check_on_receive(&uuri_1, &umessage);
        assert_eq!(check_on_receive_res, Ok(()));
    }

    #[tokio::test]
    async fn test_register_and_unregister() {
        let mut up_client_foo = UPClientFoo::default();
        let uuri_1 = uuri_factory(1);
        let listener_baz: Arc<dyn UListener> = Arc::new(ListenerBaz);
        let register_res = up_client_foo
            .register_listener(&uuri_1, None, listener_baz.clone())
            .await;
        assert!(register_res.is_ok());

        let umessage = UMessage::default();
        let check_on_receive_res = up_client_foo.check_on_receive(&uuri_1, &umessage);
        assert_eq!(check_on_receive_res, Ok(()));

        let unregister_res = up_client_foo
            .unregister_listener(&uuri_1, None, listener_baz)
            .await;
        assert!(unregister_res.is_ok());

        let umessage = UMessage::default();
        let check_on_receive_res = up_client_foo.check_on_receive(&uuri_1, &umessage);
        assert!(check_on_receive_res.is_err());
    }

    #[tokio::test]
    async fn test_register_multiple_listeners_on_one_uuri() {
        let mut up_client_foo = UPClientFoo::default();
        let uuri_1 = uuri_factory(1);
        let listener_baz: Arc<dyn UListener> = Arc::new(ListenerBaz);
        let listener_bar: Arc<dyn UListener> = Arc::new(ListenerBar);

        let register_res = up_client_foo
            .register_listener(&uuri_1, None, listener_baz.clone())
            .await;
        assert!(register_res.is_ok());

        let register_res = up_client_foo
            .register_listener(&uuri_1, None, listener_bar.clone())
            .await;
        assert!(register_res.is_ok());

        let umessage = UMessage::default();
        let check_on_receive_res = up_client_foo.check_on_receive(&uuri_1, &umessage);
        assert_eq!(check_on_receive_res, Ok(()));

        let unregister_baz_res = up_client_foo
            .unregister_listener(&uuri_1, None, listener_baz)
            .await;
        assert!(unregister_baz_res.is_ok());
        let unregister_bar_res = up_client_foo
            .unregister_listener(&uuri_1, None, listener_bar)
            .await;
        assert!(unregister_bar_res.is_ok());

        let check_on_receive_res = up_client_foo.check_on_receive(&uuri_1, &umessage);
        assert!(check_on_receive_res.is_err());
    }

    #[test]
    fn test_if_no_listeners() {
        let mut up_client_foo = UPClientFoo::default();
        let uuri_1 = uuri_factory(1);

        let umessage = UMessage::default();
        let check_on_receive_res = up_client_foo.check_on_receive(&uuri_1, &umessage);

        assert!(check_on_receive_res.is_err());
    }

    #[tokio::test]
    async fn test_register_multiple_same_listeners_on_one_uuri() {
        let mut up_client_foo = UPClientFoo::default();
        let uuri_1 = uuri_factory(1);

        let listener_baz: Arc<dyn UListener> = Arc::new(ListenerBaz);
        let register_res = up_client_foo
            .register_listener(&uuri_1, None, listener_baz.clone())
            .await;
        assert!(register_res.is_ok());

        let register_res = up_client_foo
            .register_listener(&uuri_1, None, listener_baz.clone())
            .await;
        assert!(register_res.is_err());

        let listener_baz_completely_different: Arc<dyn UListener> = Arc::new(ListenerBaz);
        let register_res = up_client_foo
            .register_listener(&uuri_1, None, listener_baz_completely_different.clone())
            .await;
        assert!(register_res.is_ok());

        let umessage = UMessage::default();
        let check_on_receive_res = up_client_foo.check_on_receive(&uuri_1, &umessage);
        assert_eq!(check_on_receive_res, Ok(()));

        let unregister_res = up_client_foo
            .unregister_listener(&uuri_1, None, listener_baz_completely_different)
            .await;
        assert!(unregister_res.is_ok());

        let unregister_baz_res = up_client_foo
            .unregister_listener(&uuri_1, None, listener_baz)
            .await;
        assert!(unregister_baz_res.is_ok());

        let check_on_receive_res = up_client_foo.check_on_receive(&uuri_1, &umessage);
        assert!(check_on_receive_res.is_err());
    }

    #[test]
    fn test_comparable_listener_pointer_address() {
        let bar = Arc::new(ListenerBar);
        let comp_listener = ComparableListener::new(bar);

        let comp_listener_thread = comp_listener.clone();
        let handle = thread::spawn(move || comp_listener_thread.pointer_address());

        let comp_listener_address_other_thread = handle.join().unwrap();
        let comp_listener_address_this_thread = comp_listener.pointer_address();

        assert_eq!(
            comp_listener_address_this_thread,
            comp_listener_address_other_thread
        );
    }

    #[test]
    fn test_comparable_listener_debug_outputs() {
        let bar = Arc::new(ListenerBar);
        let comp_listener = ComparableListener::new(bar);
        let debug_output = format!("{comp_listener:?}");
        assert!(!debug_output.is_empty());
    }
}
