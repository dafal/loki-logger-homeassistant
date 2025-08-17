# Loki Logger Home Assistant Integration

[![GitHub Release][releases-shield]][releases]
[![GitHub Activity][commits-shield]][commits]
[![License][license-shield]](LICENSE)
[![hacs][hacsbadge]][hacs]

A Home Assistant integration that streams events to Grafana Loki for centralized logging and monitoring.

## Features

- **Event Batching**: Efficiently batch Home Assistant events before sending to Loki
- **Health Monitoring**: Continuous monitoring of Loki server health with metrics
- **Retry Logic**: Intelligent retry mechanism with exponential backoff for reliability
- **Configuration UI**: User-friendly configuration through Home Assistant's interface
- **Multi-language Support**: Available in English and French

## Installation

### HACS (Recommended)

1. Open HACS in your Home Assistant instance
2. Click on "Integrations"
3. Click the three dots in the top right corner and select "Custom repositories"
4. Add this repository URL: `https://github.com/dafal/loki-logger-homeassistant`
5. Select "Integration" as the category
6. Click "Add"
7. Search for "Loki Logger" and install

### Manual Installation

1. Using the tool of choice open the directory (folder) for your HA configuration (where you find `configuration.yaml`)
2. If you do not have a `custom_components` directory (folder) there, you need to create it
3. In the `custom_components` directory (folder) create a new folder called `loki`
4. Download _all_ the files from the `custom_components/loki/` directory (folder) in this repository
5. Place the files you downloaded in the new directory (folder) you created
6. Restart Home Assistant
7. In the HA UI go to "Configuration" -> "Integrations" click "+" and search for "Loki Logger"

## Configuration

The integration can be configured through the Home Assistant UI:

1. Go to **Configuration** → **Integrations**
2. Click **Add Integration**
3. Search for **Loki Logger**
4. Follow the configuration steps

### Configuration Options

- **Host**: Your Loki server hostname or IP address
- **Port**: Loki server port (default: 3100)
- **Bearer Token**: Authentication token for Loki
- **SSL**: Enable HTTPS connection
- **Verify SSL**: Verify SSL certificates
- **Batch Size**: Number of events to batch before sending (1-1000)
- **Batch Timeout**: Maximum time to wait before sending batch (1-60 seconds)
- **Health Monitoring**: Enable periodic health checks
- **Retry Logic**: Enable automatic retry on failures

## Sensors

The integration provides the following sensors:

- **Loki Health Status**: Current health status of your Loki server
- **Loki Metrics**: Detailed metrics including success rate, events processed, and response times

## Contributing

If you want to contribute to this please read the [Contribution guidelines](CONTRIBUTING.md)

## Credits

This project was generated from [@oncleben31](https://github.com/oncleben31)'s [Home Assistant Custom Component Cookiecutter](https://github.com/oncleben31/cookiecutter-homeassistant-custom-component) template.

Code template was mainly taken from [@Ludeeus](https://github.com/ludeeus)'s [integration_blueprint][integration_blueprint] template

---

[integration_blueprint]: https://github.com/custom-components/integration_blueprint
[commits-shield]: https://img.shields.io/github/commit-activity/y/dafal/loki-logger-homeassistant.svg?style=for-the-badge
[commits]: https://github.com/dafal/loki-logger-homeassistant/commits/main
[hacs]: https://github.com/hacs/integration
[hacsbadge]: https://img.shields.io/badge/HACS-Custom-orange.svg?style=for-the-badge
[exampleimg]: example.png
[license-shield]: https://img.shields.io/github/license/dafal/loki-logger-homeassistant.svg?style=for-the-badge
[releases-shield]: https://img.shields.io/github/release/dafal/loki-logger-homeassistant.svg?style=for-the-badge
[releases]: https://github.com/dafal/loki-logger-homeassistant/releases