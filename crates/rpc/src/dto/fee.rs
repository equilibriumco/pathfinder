use super::U256Hex;

impl crate::dto::SerializeForVersion for pathfinder_executor::types::FeeEstimate {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        let mut serializer = serializer.serialize_struct()?;

        serializer.serialize_field("l1_gas_consumed", &U256Hex(self.l1_gas_consumed))?;
        serializer.serialize_field("l1_gas_price", &U256Hex(self.l1_gas_price))?;
        serializer.serialize_field("l1_data_gas_consumed", &U256Hex(self.l1_data_gas_consumed))?;
        serializer.serialize_field("l1_data_gas_price", &U256Hex(self.l1_data_gas_price))?;
        serializer.serialize_field("l2_gas_consumed", &U256Hex(self.l2_gas_consumed))?;
        serializer.serialize_field("l2_gas_price", &U256Hex(self.l2_gas_price))?;
        serializer.serialize_field("overall_fee", &U256Hex(self.overall_fee))?;
        serializer.serialize_field("unit", &self.unit)?;
        serializer.end()
    }
}

impl crate::dto::SerializeForVersion for pathfinder_executor::types::PriceUnit {
    fn serialize(
        &self,
        serializer: crate::dto::Serializer,
    ) -> Result<crate::dto::Ok, crate::dto::Error> {
        serializer.serialize_str(match self {
            pathfinder_executor::types::PriceUnit::Wei => "WEI",
            pathfinder_executor::types::PriceUnit::Fri => "FRI",
        })
    }
}

#[cfg(test)]
mod tests {
    use pathfinder_executor::types::{FeeEstimate, PriceUnit};
    use primitive_types::U256;

    use super::*;
    use crate::dto::SerializeForVersion;

    #[test]
    fn fee_estimate_serialization() {
        let fee = FeeEstimate {
            l1_gas_consumed: U256::from(100),
            l1_gas_price: U256::from(50),
            l1_data_gas_consumed: U256::from(200),
            l1_data_gas_price: U256::from(25),
            l2_gas_consumed: U256::from(300),
            l2_gas_price: U256::from(10),
            overall_fee: U256::from(1000),
            unit: PriceUnit::Wei,
        };

        let serializer = crate::dto::Serializer::new(crate::dto::RpcVersion::V09);
        let result = serde_json::to_value(fee.serialize(serializer).unwrap()).unwrap();

        let expected = serde_json::json!({
            "l1_gas_consumed": "0x64",       // 100
            "l1_gas_price": "0x32",          // 50
            "l1_data_gas_consumed": "0xc8",  // 200
            "l1_data_gas_price": "0x19",     // 25
            "l2_gas_consumed": "0x12c",      // 300
            "l2_gas_price": "0xa",           // 10
            "overall_fee": "0x3e8",          // 1000
            "unit": "WEI"
        });

        assert_eq!(result, expected);
    }
}
