# Tests for fluxkv.
#
# Tests are added here as the server is split into testable pieces. The
# protocol layer is the first target: it parses and builds mcbp frames with
# no magma or socket dependency, so it can be tested on its own.
