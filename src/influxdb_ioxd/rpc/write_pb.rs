use super::error::default_server_error_handler;
use generated_types::google::FieldViolation;
use generated_types::influxdata::pbdata::v1::*;
use server::{connection::ConnectionManager, Server};
use std::fmt::Debug;
use std::sync::Arc;

struct PBWriteService<M: ConnectionManager> {
    server: Arc<Server<M>>,
}

#[tonic::async_trait]
impl<M> write_service_server::WriteService for PBWriteService<M>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    async fn write(
        &self,
        request: tonic::Request<WriteRequest>,
    ) -> Result<tonic::Response<WriteResponse>, tonic::Status> {
        let span_ctx = request.extensions().get().cloned();
        let database_batch = request
            .into_inner()
            .database_batch
            .ok_or_else(|| FieldViolation::required("database_batch"))?;

        self.server
            .write_pb(database_batch, span_ctx)
            .await
            .map_err(default_server_error_handler)?;

        Ok(tonic::Response::new(WriteResponse {}))
    }
}

pub fn make_server<M>(
    server: Arc<Server<M>>,
) -> write_service_server::WriteServiceServer<impl write_service_server::WriteService>
where
    M: ConnectionManager + Send + Sync + Debug + 'static,
{
    write_service_server::WriteServiceServer::new(PBWriteService { server })
}
