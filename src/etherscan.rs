//! Etherscan API client for fetching verified contract ABIs.

use std::time::Duration;

/// Contract information fetched from Etherscan.
#[derive(Debug)]
pub struct ContractInfo {
    /// Contract name from Etherscan (proxy name if proxy, implementation name otherwise).
    pub name: String,
    /// ABI JSON string (from implementation if proxy).
    pub abi_json: String,
    /// Whether the contract is a proxy (implementation ABI was used).
    pub is_proxy: bool,
}

/// Fetch contract info from Etherscan, following proxy if detected.
///
/// If the contract is a proxy (has a non-empty `Implementation` field),
/// the implementation's ABI is fetched and the proxy's name is kept.
///
/// # Errors
///
/// Returns an error if the HTTP request fails, the contract is not verified,
/// or the response cannot be parsed.
pub async fn fetch_contract_info(address: &str, api_key: &str) -> eyre::Result<ContractInfo> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap_or_default();

    let (name, abi_json, implementation) = fetch_source_code(&client, address, api_key).await?;

    // If proxy, fetch implementation ABI
    if !implementation.is_empty() {
        let (_, impl_abi, _) = fetch_source_code(&client, &implementation, api_key).await?;
        return Ok(ContractInfo {
            name,
            abi_json: impl_abi,
            is_proxy: true,
        });
    }

    Ok(ContractInfo {
        name,
        abi_json,
        is_proxy: false,
    })
}

/// Fetch the block number at which a contract was deployed.
///
/// Uses the Etherscan V2 `getcontractcreation` endpoint.
/// Returns `None` if the creation info is unavailable.
///
/// # Errors
///
/// Returns an error if the HTTP request fails or the response cannot be parsed.
pub async fn fetch_creation_block(address: &str, api_key: &str) -> eyre::Result<Option<u64>> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap_or_default();

    let url = format!(
        "https://api.etherscan.io/v2/api?chainid=1&module=contract&action=getcontractcreation&contractaddresses={address}&apikey={api_key}"
    );

    let resp: serde_json::Value = client
        .get(&url)
        .send()
        .await
        .map_err(|e| eyre::eyre!("etherscan request failed: {e}"))?
        .json()
        .await
        .map_err(|e| eyre::eyre!("etherscan response parse failed: {e}"))?;

    parse_creation_response(&resp)
}

/// Parse the Etherscan `getcontractcreation` JSON response.
fn parse_creation_response(resp: &serde_json::Value) -> eyre::Result<Option<u64>> {
    let status = resp["status"].as_str().unwrap_or("0");
    if status != "1" {
        return Ok(None);
    }

    let block_str = resp["result"]
        .as_array()
        .and_then(|arr| arr.first())
        .and_then(|r| r["blockNumber"].as_str());

    match block_str {
        Some(s) => {
            let block = s
                .parse::<u64>()
                .map_err(|e| eyre::eyre!("failed to parse creation block number: {e}"))?;
            Ok(Some(block))
        }
        None => Ok(None),
    }
}

/// Fetch raw source info for a single address.
///
/// Returns `(contract_name, abi_json, implementation_address)`.
async fn fetch_source_code(
    client: &reqwest::Client,
    address: &str,
    api_key: &str,
) -> eyre::Result<(String, String, String)> {
    let url = format!(
        "https://api.etherscan.io/v2/api?chainid=1&module=contract&action=getsourcecode&address={address}&apikey={api_key}"
    );

    let resp: serde_json::Value = client
        .get(&url)
        .send()
        .await
        .map_err(|e| eyre::eyre!("etherscan request failed: {e}"))?
        .json()
        .await
        .map_err(|e| eyre::eyre!("etherscan response parse failed: {e}"))?;

    parse_source_code_response(&resp)
}

/// Parse the Etherscan `getsourcecode` JSON response.
///
/// # Errors
///
/// Returns an error if the response status is not "1", the ABI indicates
/// the contract is not verified, or required fields are missing.
fn parse_source_code_response(resp: &serde_json::Value) -> eyre::Result<(String, String, String)> {
    let status = resp["status"].as_str().unwrap_or("0");
    if status != "1" {
        let message = resp["message"].as_str().unwrap_or("unknown error");
        return Err(eyre::eyre!("etherscan API error: {message}"));
    }

    let result = resp["result"]
        .as_array()
        .and_then(|arr| arr.first())
        .ok_or_else(|| eyre::eyre!("etherscan returned empty result array"))?;

    let abi_json = result["ABI"]
        .as_str()
        .ok_or_else(|| eyre::eyre!("etherscan response missing ABI field"))?;

    if abi_json == "Contract source code not verified" {
        return Err(eyre::eyre!(
            "contract source code not verified on Etherscan"
        ));
    }

    let name = result["ContractName"].as_str().unwrap_or("").to_owned();

    let implementation = result["Implementation"].as_str().unwrap_or("").to_owned();

    Ok((name, abi_json.to_owned(), implementation))
}

#[cfg(test)]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions in tests are idiomatic"
)]
mod tests {
    use super::*;

    #[test]
    fn parse_verified_contract() -> eyre::Result<()> {
        let json: serde_json::Value = serde_json::from_str(
            r#"{
                "status": "1",
                "message": "OK",
                "result": [{
                    "ContractName": "FiatTokenV2_2",
                    "ABI": "[{\"type\":\"event\",\"name\":\"Transfer\"}]",
                    "Implementation": ""
                }]
            }"#,
        )?;

        let (name, abi, implementation) = parse_source_code_response(&json)?;
        assert_eq!(name, "FiatTokenV2_2");
        assert_eq!(abi, r#"[{"type":"event","name":"Transfer"}]"#);
        assert!(implementation.is_empty());
        Ok(())
    }

    #[test]
    fn parse_proxy_contract() -> eyre::Result<()> {
        let json: serde_json::Value = serde_json::from_str(
            r#"{
                "status": "1",
                "message": "OK",
                "result": [{
                    "ContractName": "FiatTokenProxy",
                    "ABI": "[{\"type\":\"function\",\"name\":\"admin\"}]",
                    "Implementation": "0x43506849D7C04F9138D1A2050bbF3A0c054402dd"
                }]
            }"#,
        )?;

        let (name, _abi, implementation) = parse_source_code_response(&json)?;
        assert_eq!(name, "FiatTokenProxy");
        assert_eq!(implementation, "0x43506849D7C04F9138D1A2050bbF3A0c054402dd");
        Ok(())
    }

    #[test]
    fn unverified_contract_error() -> eyre::Result<()> {
        let json: serde_json::Value = serde_json::from_str(
            r#"{
                "status": "1",
                "message": "OK",
                "result": [{
                    "ContractName": "",
                    "ABI": "Contract source code not verified",
                    "Implementation": ""
                }]
            }"#,
        )?;

        let result = parse_source_code_response(&json);
        assert!(result.is_err());
        assert!(
            format!("{result:?}").contains("not verified"),
            "error should mention 'not verified'"
        );
        Ok(())
    }

    #[test]
    fn api_error_status() -> eyre::Result<()> {
        let json: serde_json::Value = serde_json::from_str(
            r#"{
                "status": "0",
                "message": "NOTOK",
                "result": "Invalid API Key"
            }"#,
        )?;

        let result = parse_source_code_response(&json);
        assert!(result.is_err());
        assert!(
            format!("{result:?}").contains("NOTOK"),
            "error should contain API message"
        );
        Ok(())
    }

    #[test]
    fn parse_creation_block() -> eyre::Result<()> {
        let json: serde_json::Value = serde_json::from_str(
            r#"{
                "status": "1",
                "message": "OK",
                "result": [{
                    "contractAddress": "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
                    "contractCreator": "0x95ba4cf87d6723ad9c0db21737d862be44a680de",
                    "txHash": "0x2f1c5c2b44f771e942a8506148e256f94f1a464babc938ae0690c6e34cd79190",
                    "blockNumber": "6082465"
                }]
            }"#,
        )?;

        let block = parse_creation_response(&json)?;
        assert_eq!(block, Some(6_082_465));
        Ok(())
    }

    #[test]
    fn parse_creation_block_not_found() -> eyre::Result<()> {
        let json: serde_json::Value = serde_json::from_str(
            r#"{
                "status": "0",
                "message": "No data found",
                "result": []
            }"#,
        )?;

        let block = parse_creation_response(&json)?;
        assert_eq!(block, None);
        Ok(())
    }
}
