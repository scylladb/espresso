use aws_sdk_dynamodb::config::endpoint::{Endpoint, Params, ResolveEndpoint};
use aws_smithy_runtime_api::client::endpoint::EndpointFuture;

use crate::node_router::NodeRouter;

/// Implements the AWS SDK's `ResolveEndpoint` trait so that every DynamoDB
/// request is directed to the next Alternator node in round-robin order.
///
/// The endpoint is resolved *before* SigV4 signing, so the correct host is
/// embedded in the Authorization header.  Alternator does not verify the
/// signature cryptographically for dummy credentials, but the host must still
/// be consistent between the resolved URL and the signed host.
#[derive(Debug, Clone)]
pub struct RoundRobinResolver {
    router: NodeRouter,
}

impl RoundRobinResolver {
    pub fn new(router: NodeRouter) -> Self {
        Self { router }
    }
}

impl ResolveEndpoint for RoundRobinResolver {
    fn resolve_endpoint<'a>(&'a self, _params: &'a Params) -> EndpointFuture<'a> {
        let node = self.router.next_node().to_owned();
        EndpointFuture::ready(Ok(Endpoint::builder().url(node).build()))
    }
}
