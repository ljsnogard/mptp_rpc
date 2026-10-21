use abs_mptp::serving::TrReqHandler;
use mptp_smux::serving::TrPathRouterMut;

use crate::{
    serving::config::TrServingConfig,
    x_deps::{abs_mptp, mptp_smux},
};

pub trait TrRpcServiceRouter<C>
where
    C: TrServingConfig,
    Self: TrReqHandler<C>,
{
    type Router: TrPathRouterMut<Handler: TrReqHandler<C>>;

    const INTERFACE_PATH: &'static str;

    fn router_mut(&mut self) -> &mut Self::Router;

    fn router(&self) -> &Self::Router;
}
