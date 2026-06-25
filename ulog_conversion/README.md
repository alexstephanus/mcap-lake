# Ulog Conversion

This package handles conversion of raw `.ulg` files into `.mcap`.
It currently has one limitation, in that it doesn't support nested messages.
This means that:
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