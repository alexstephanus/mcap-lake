# Foxglove

This folder contains the Foxglove dashboard JSON, and the below documentation:

## Importing and Populating the Dashboard

### Importing the Dashboard

1. Open Foxglove (either desktop or webapp) and go to the Layouts panel
2. Install the "PX4 Converter" extension (this is a public extension, you can use it with a free account)
3. Click "Add" -> "Import Personal Layout", and import the `px4_flight_analysis.json` file from this folder
4. Open that new layout.  It will be unpopulated

### Populating the Dashboard
This pipeline stores files in MinIO, so for the moment you'll need to download the MCAP files from
MinIO to access them in Foxglove.  To download files, ensure that the MinIO container is running and:

1. Navigate to (http://localhost:9001) in your browser, and log in using `admin`/`password` (this is a huge vulnerability, anyone who has access to everything on your computer already could just _log in_ and download sample MCAPs!)
2. Download a file from the `mcap` bucket (the larger the better, as far as I'm concerned.  More to look at)
3. Drag that downloaded file onto the unpopulated dashboard in Foxglove

## Dashboard Docs
A few things to know about the dashboard:

- **Install the PX4 Extension:** The dashboard requires the "PX4 Converter" extension to render the drone's current 3D coordinates.  This is a published extension and doesn't require a paid seat to install, but it is necessary to get the full functionality of the dashboard
- **PX4 Version:** The dashboard is pinned to the 1.17.0 PX4 firmware version (git hash `d6f12ad1c4f70ad3230afd7d86e971421e02fef4`), other firmware versions will load, but may not be accurately reflected: either due to changes to channel names/values, or changes to state transition enum meanings (these are unlikely, but I am flagging them nonetheless)
- **Flight Path:** The complete flight path will only render in the "3D View" panel _after_ the flight starts.  If there's a boot -> flight start delay, the flight path will not render during that gap
- **Failsafe Granularity:** There are a bunch of failsafes that can be triggered, and the failsafe indication in "State transitions" is a simple boolean flag that lets you know if _any_ failsafe is triggered.  To dig deeper, you can add indicators for specific failsafes or dig into the raw MCAP 
- **Remaining Battery:** The "Remaining" series in the "Battery Voltage & Capacity" chart is just an estimate, not an actual measured value.  There are more concrete values that PX4 publishes, but they're (as far as I know) dependent on the specific BMS to populate.  So, we stick with the estimate for universality