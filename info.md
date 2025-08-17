{% if installed %}
## Changes as compared to your installed version:

### Breaking Changes

### Changes

### Features

- Event batching for improved performance
- Health monitoring with periodic checks
- Intelligent retry logic with exponential backoff
- Configuration UI for easy setup
- Multi-language support (English and French)
- Detailed metrics and health sensors

### Bugfixes

---

{% endif %}

# Loki Logger

[![GitHub Release][releases-shield]][releases]
[![GitHub Activity][commits-shield]][commits]
[![License][license-shield]][license]
[![hacs][hacsbadge]][hacs]

_Integration to stream Home Assistant events to Grafana Loki for centralized logging and monitoring._

**This integration will set up the following platforms.**

Platform | Description
-- | --
`sensor` | Show health status and metrics for your Loki connection.

## Features

- **Event Batching**: Efficiently batch Home Assistant events before sending to Loki
- **Health Monitoring**: Continuous monitoring of Loki server health with metrics  
- **Retry Logic**: Intelligent retry mechanism with exponential backoff for reliability
- **Configuration UI**: User-friendly configuration through Home Assistant's interface
- **Multi-language Support**: Available in English and French

## Installation

1. Using the tool of choice open the directory (folder) for your HA configuration (where you find `configuration.yaml`)
2. If you do not have a `custom_components` directory (folder) there, you need to create it
3. In the `custom_components` directory (folder) create a new folder called `loki`
4. Download _all_ the files from the `custom_components/loki/` directory (folder) in this repository
5. Place the files you downloaded in the new directory (folder) you created
6. Restart Home Assistant
7. In the HA UI go to "Configuration" -> "Integrations" click "+" and search for "Loki Logger"

## Configuration is done in the UI

<!---->

***

[integration_blueprint]: https://github.com/custom-components/integration_blueprint
[commits-shield]: https://img.shields.io/github/commit-activity/y/dafal/loki-logger-homeassistant.svg?style=for-the-badge
[commits]: https://github.com/dafal/loki-logger-homeassistant/commits/main
[hacs]: https://github.com/hacs/integration
[hacsbadge]: https://img.shields.io/badge/HACS-Custom-orange.svg?style=for-the-badge
[license-shield]: https://img.shields.io/github/license/dafal/loki-logger-homeassistant.svg?style=for-the-badge
[license]: https://github.com/dafal/loki-logger-homeassistant/blob/main/LICENSE
[releases-shield]: https://img.shields.io/github/release/dafal/loki-logger-homeassistant.svg?style=for-the-badge
[releases]: https://github.com/dafal/loki-logger-homeassistant/releases