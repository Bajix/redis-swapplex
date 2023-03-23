use quote::quote;
use syn::{parse_macro_input, DeriveInput};

#[proc_macro_derive(ConnectionManagerContext)]
pub fn derive_manager_context(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
  let input = parse_macro_input!(input as DeriveInput);
  let ident = &input.ident;

  let expanded = quote! {
    impl redis_swapplex::ConnectionManagerContext for #ident {
      type ConnectionInfo = redis_swapplex::RedisDB<#ident>;
      fn connection_manager() -> &'static redis_swapplex::ConnectionManager<redis_swapplex::RedisDB<#ident>> {
        static CONNECTION_MANAGER: redis_swapplex::ConnectionManager<redis_swapplex::RedisDB<#ident>> =
        redis_swapplex::ConnectionManager::new(<redis_swapplex::RedisDB<#ident>>::default);

        &CONNECTION_MANAGER
      }

      fn state_cache(
      ) -> &'static std::thread::LocalKey<std::cell::RefCell<redis_swapplex::arc_swap::Cache<&'static redis_swapplex::arc_swap::ArcSwapOption<redis_swapplex::ConnectionState>, Option<std::sync::Arc<redis_swapplex::ConnectionState>>>>>
      {
        thread_local! {
          static STATE_CACHE:
          std::cell::RefCell<redis_swapplex::arc_swap::Cache<&'static redis_swapplex::arc_swap::ArcSwapOption<redis_swapplex::ConnectionState>, Option<std::sync::Arc<redis_swapplex::ConnectionState>>>> = std::cell::RefCell::new(redis_swapplex::arc_swap::Cache::new(<#ident>::connection_manager().deref()));
        }

        &STATE_CACHE
      }
    }
  };

  expanded.into()
}
