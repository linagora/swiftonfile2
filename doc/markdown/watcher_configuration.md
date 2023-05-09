# Watcher Configuration


## Overview

Watcher is a new feature of Swift on File 2.
It allows to update the Swift containers automatically when the underlying FS is updated.

It currently works with all POSIX compliant filesystem with a `Generic` driver.
Moreover, specific drivers can be added to increase performance on specific FS.
For example, it currently exists two drivers: the `Generic` driver and the `Lustre` driver (for Lustre FS).


## Generic driver configuration

The generic driver can be configured with several parameters:

In `object-server.conf`:

```
[app:object-server]
use = egg:swiftonfile#object
watcher_interval = 60
watcher_nb_threads = 20
```

- The `watcher_interval` parameter means that the generic driver will scan the fs every minute.
- The `watcher_nb_threads` permits to increase the number of threads (default value: 10)


## Lustre driver configuration

In order to use the lustre driver, new parameters are used:

In `object-server.conf`:

```
[app:object-server]
use = egg:swiftonfile#object
watcher_interval = 1
watch_driver = swiftonfile.watcher.drivers.lustre.LustreWatcher
force_sync_at_boot = false
watcher_lustre_changelog_quantity = 2000
```

- The `watcher_interval` parameter can be set to 1 second for Lustre. It means that the Lustre changelogs will be retrieved every second.
- The `watch_driver` parameter must be set to the driver module (here it's Lustre).
- The `force_sync_at_boot` parameter forces a full FS scan when starting the service. The default value is `false`.
- The `watcher_lustre_changelog_quantity` parameter is the quantity of changelog retrieved at each `watcher_interval` iteration. The default value is 2000.
