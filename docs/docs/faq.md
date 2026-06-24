---
sidebar_position: 7
slug: /faq
---

# Frequently Asked Questions

This section addresses common issues and questions that might arise while running or developing with Pathfinder. If you don’t find your answer here, consider searching the [GitHub issues](https://github.com/equilibriumco/pathfinder/issues) or asking in the [Starknet Discord channel](https://discord.com/invite/QypNMzkHbc).

<details>
<summary><strong>What is Pathfinder?</strong></summary>

Pathfinder is a full node implementation designed to interact with the Starknet blockchain, providing a secure and reliable view of the network.
</details>

<details>
<summary><strong>How can I run Pathfinder?</strong></summary>

You can run Pathfinder using Docker or by building it from source. Detailed instructions for both methods are provided in the [installation section](getting-started/running-pathfinder).
</details>

<details>
<summary><strong>What are the hardware requirements for running Pathfinder?</strong></summary>

See the [Hardware Requirements](./getting-started/hardware-requirements.md) page for the recommended configuration.
</details>

<details>
<summary><strong>How can I configure my Pathfinder node?</strong></summary>

Pathfinder can be configured using environment variables or command-line options, such as specifying the network, logging level, and Ethereum API URL. You can find more details in the [configuration section](getting-started/configuration).
</details>

<details>
<summary><strong>How can I update my Pathfinder node?</strong></summary>

Check out the [Updating Pathfinder](getting-started/updating-pathfinder) guide for detailed instructions on updating your Pathfinder node to the latest version.
</details>

<details>
<summary><strong>How can I interact with my Pathfinder node?</strong></summary>

You can interact with Pathfinder using the [JSON-RPC API](interacting-with-pathfinder/json-rpc-api) or the [WebSocket API](interacting-with-pathfinder/websocket-api) for real-time updates. These APIs allow you to query the blockchain, submit transactions, and subscribe to events.
</details>

<details>
<summary><strong>Can I switch from archive mode to pruned mode (or vice versa) without re-syncing?</strong></summary>

Currently, you cannot switch directly between archive and pruned modes mid-run. You may, however, change the k value in pruned mode between runs. If you need to go from archive to pruned, consider downloading a pruned Database Snapshot or re-sync with the `--storage.state-tries=<k>` option.
</details>

<details>
<summary><strong>How can I monitor my Pathfinder node?</strong></summary>

Use the monitoring API endpoints (`/health`, `/ready`, `/ready/synced`) and integrate Prometheus metrics to monitor the node's health and performance. For more details, refer to the [Monitoring API section](monitoring-and-metrics).
</details>

<details>
<summary><strong>Do node operators receive any rewards, or is participation solely to support the network?</strong></summary>

Currently, running a Pathfinder node is a voluntary effort to support the Starknet network, and there are no direct rewards for node operators.
</details>

<details>
<summary><strong>How can I view Pathfinder logs from Docker?</strong></summary>

You can view the logs of your Pathfinder Docker container using the command: `docker logs -f pathfinder`.
</details>

<details>
<summary><strong>How can I get real-time updates of new blocks?</strong></summary>

Use the WebSocket API to subscribe to the `newHeads` event, which provides real-time notifications for new blocks added to the blockchain.
</details>

<details>
<summary><strong>Does Pathfinder provide snapshots to sync with Starknet quickly?</strong></summary>

Yes, Pathfinder provides database snapshots that can be downloaded and used to speed up the syncing process. Refer to the [snapshots section](database-snapshots) for more details.
</details>

<details>
<summary><strong>How can I contribute to Pathfinder?</strong></summary>

You can contribute by opening issues, submitting pull requests, or joining the Starknet community on Discord to provide feedback and collaborate with other developers. For more details, refer to the [contribution guidelines](https://github.com/equilibriumco/pathfinder/blob/main/contributing.md).
</details>

<details>
<summary><strong>Can I use Pathfinder with a custom Starknet network?</strong></summary>

Yes, you can configure Pathfinder to connect to a custom network by specifying the `--network custom` command line option and providing the appropriate gateway URLs. For more details, refer to the [custom network section](getting-started/configuration#custom-networks-and-gateway-proxies).
</details>

<details>
<summary><strong>How do I monitor my Pathfinder node?</strong></summary>

Use the monitoring API endpoints (`/health`, `/ready`, `/ready/synced`) and integrate Prometheus metrics for detailed monitoring. For more information, refer to the [Monitoring API section](monitoring-and-metrics).
</details>

<details>
<summary><strong>What should I do if my node is not syncing?</strong></summary>

Ensure your Ethereum endpoint is accessible and has archive capabilities, and verify that your hardware meets the performance requirements.
</details>
