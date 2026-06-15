use crate::context::RpcContext;
use crate::method::add_invoke_transaction::{AddInvokeTransactionError, Input, Output};

pub async fn add_invoke_transaction(
    context: RpcContext,
    input: Input,
) -> Result<Output, AddInvokeTransactionError> {
    if !input.is_v3_transaction() {
        return Err(AddInvokeTransactionError::UnsupportedTransactionVersion);
    }

    crate::method::add_invoke_transaction::add_invoke_transaction(context, input).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::RpcContext;

    #[rstest::rstest]
    #[case::v0_is_unsupported(Input::for_test_with_v0_transaction(), false)]
    #[case::v1_is_unsupported(Input::for_test_with_v1_transaction(), false)]
    #[case::v3_is_supported(Input::for_test_with_v3_transaction(), true)]
    #[tokio::test]
    async fn only_v3_transactions_are_accepted(#[case] input: Input, #[case] is_supported: bool) {
        let context = RpcContext::for_tests();
        let result = add_invoke_transaction(context, input).await;
        assert_eq!(
            !is_supported,
            matches!(
                result,
                Err(AddInvokeTransactionError::UnsupportedTransactionVersion)
            )
        );
    }
}
