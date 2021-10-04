#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Status {
    #[prost(int32, tag = "1")]
    pub code: i32,
    #[prost(string, tag = "2")]
    pub message: ::prost::alloc::string::String,
    #[prost(message, repeated, tag = "3")]
    pub details: ::prost::alloc::vec::Vec<::pbjson_types::Any>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RetryInfo {
    #[prost(message, optional, tag = "1")]
    pub retry_delay: ::core::option::Option<::pbjson_types::Duration>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct DebugInfo {
    #[prost(string, repeated, tag = "1")]
    pub stack_entries: ::prost::alloc::vec::Vec<::prost::alloc::string::String>,
    #[prost(string, tag = "2")]
    pub detail: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct QuotaFailure {
    #[prost(message, repeated, tag = "1")]
    pub violations: ::prost::alloc::vec::Vec<quota_failure::Violation>,
}
/// Nested message and enum types in `QuotaFailure`.
pub mod quota_failure {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Violation {
        #[prost(string, tag = "1")]
        pub subject: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub description: ::prost::alloc::string::String,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ErrorInfo {
    #[prost(string, tag = "1")]
    pub reason: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub domain: ::prost::alloc::string::String,
    #[prost(map = "string, string", tag = "3")]
    pub metadata:
        ::std::collections::HashMap<::prost::alloc::string::String, ::prost::alloc::string::String>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct PreconditionFailure {
    #[prost(message, repeated, tag = "1")]
    pub violations: ::prost::alloc::vec::Vec<precondition_failure::Violation>,
}
/// Nested message and enum types in `PreconditionFailure`.
pub mod precondition_failure {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Violation {
        #[prost(string, tag = "1")]
        pub r#type: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub subject: ::prost::alloc::string::String,
        #[prost(string, tag = "3")]
        pub description: ::prost::alloc::string::String,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BadRequest {
    #[prost(message, repeated, tag = "1")]
    pub field_violations: ::prost::alloc::vec::Vec<bad_request::FieldViolation>,
}
/// Nested message and enum types in `BadRequest`.
pub mod bad_request {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct FieldViolation {
        #[prost(string, tag = "1")]
        pub field: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub description: ::prost::alloc::string::String,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct RequestInfo {
    #[prost(string, tag = "1")]
    pub request_id: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub serving_data: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ResourceInfo {
    #[prost(string, tag = "1")]
    pub resource_type: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub resource_name: ::prost::alloc::string::String,
    #[prost(string, tag = "3")]
    pub owner: ::prost::alloc::string::String,
    #[prost(string, tag = "4")]
    pub description: ::prost::alloc::string::String,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Help {
    #[prost(message, repeated, tag = "1")]
    pub links: ::prost::alloc::vec::Vec<help::Link>,
}
/// Nested message and enum types in `Help`.
pub mod help {
    #[derive(Clone, PartialEq, ::prost::Message)]
    pub struct Link {
        #[prost(string, tag = "1")]
        pub description: ::prost::alloc::string::String,
        #[prost(string, tag = "2")]
        pub url: ::prost::alloc::string::String,
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct LocalizedMessage {
    #[prost(string, tag = "1")]
    pub locale: ::prost::alloc::string::String,
    #[prost(string, tag = "2")]
    pub message: ::prost::alloc::string::String,
}
