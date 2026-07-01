# Ulog Conversion

This package handles conversion of raw `.ulg` files into `.mcap`.
There are a couple callouts you should be aware of:

## Data modifications
We make some new channels based on the ULog data as we convert it to MCAP.  Most of it has to do with Foxglove compatibility.  The following new channels are introduced:

- **vehicle_trajectory_foxglove:** We aggregate all the vehicle_local_position logs into one trajectory message for flight-path display in Foxglove.  Coordinates are converted from NED to ENU.
- **logged_messages:** Logs (tagged and untagged) are encoded using the Foxglove log schema.  Level remapping is lossy, see `_PX4_TO_FOXGLOVE_LOG_LEVEL` in convert.py for more details
- **parameter_changes:** All parameter changes get written to their own MCAP channel

Additionally, we make some structural changes:

- **Channel Splitting:** ULog message definitions (akin to MCAP channels) have a multi_id associated with them to differentiate between sources, to support e.g. multiple sensors of the same type.  When converting to MCAP we separate them out into multiple channels that share a schema definition.
- **Nested Array Reformatting:** Documented below

## No nested message support
The converter doesn't support nested messages right now.  This means that:
1. Any sort of array-of-arrays gets dropped with a warning log emitted.  The exception to this is
if the leaf array is an array of chars, which are treated as a string rather than a sub-array.
2. Arrays of structs get each individual field written into the final `.mcap` as
a list of the corresponding primitive type.  So, something like
```
[
    {
        "foo": 32,
        "bar": 64,
    },
    {
        "foo": 128,
        "bar": 256,
    }
]
```
shows up in the MCAP as 
```
{
    "foo": [32, 128],
    "bar": [64, 256],
}
```

This might change, but it's not a high-priority item.
