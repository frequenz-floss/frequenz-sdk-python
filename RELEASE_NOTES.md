# Frequenz Python SDK Release Notes

## Bug Fixes

- The additive `ShiftingMatryoshka` algorithm was made for batteries, but got set as the default algorithm for PV and EV chargers.  This is now reversed and PV and EV chargers are back to using the original `Matryoshka` algorithm.
