# Swift on file Configuration

## Overview

This page completes the quick start guide and is aimed at explaining only the Swift on File configuration.

## Object server configuration

In order to enable Swift on File, the `object-server.conf` file must contains the following lines:

```
[app:object-server]
use = egg:swiftonfile#object
```

This line means that the object-server will pass through Swift on File.

## Proxy server configuration

Swift on File must be installed on proxy nodes too in order to check the filename before sending it to the object node.

To enable this feature, the `sof_constraints` must be added to the pipeline.

Here the configuration to apply on the proxy node:

```
[pipeline:main]
pipeline = catch_errors sof_constraints ...

[filter:sof_constraints]
use = egg:swiftonfile#sof_constraints
policies=swiftonfile
```

The ``sof_constraints`` middleware should be added to the pipeline in your
``/etc/swift/proxy-server.conf`` file, and a mapping of storage policies
using the swiftonfile object server should be listed in the 'policies'
variable in the filter section.

The swiftonfile constraints contains additional checks to make sure object
names conform with POSIX filesystems file and directory naming limitations.


## User mapping on FS

Swift on File 2 adds a new feature: it allows to map the Swift user to the system user on the FS.
To do so, of course you need to use a system like LDAP in order to have the same user on the FS and
on the Keystone installation.

In order to use this feature, your Swift object node must be executed with `root` user.

There are two ways to configure this feature.

### 1. Mapping from User name

Here the configuration for this operating mode:

```
[app:object-server]
use = egg:swiftonfile#object
match_fs_user = swiftonfile.swift.obj.diskfile.UserMappingDiskFileBehavior
```

This configuration is enough to make it work. When a new file is created, `chown` is called
and the user/group of the file is set to the user/group of the calling account.

---
**Warning**

What we call the user in the Swift world is the account, not the username used to connect to Swift.
---


### 2. Mapping from Group name

Here the configuration for this operating mode:

```
[app:object-server]
use = egg:swiftonfile#object
match_fs_user = swiftonfile.swift.obj.diskfile.GroupMappingDiskFileBehavior
match_fs_default_user = nobody
```

With this configuration, the account used to call Swift will be associated with the group on the FS.
If the group has a main user associated to it, the user of the file will be associated with this group.
But you could have a group without main user, and in that case, the `match_fs_default_user` will be
used (by default it's `root`).


### Others configuration options

There are two others options that can be configured:

```
[app:object-server]
use = egg:swiftonfile#object
match_fs_reseller_prefix = AUTH
match_fs_ignore_accounts = account1,account2
```

- The `match_fs_reseller_prefix` option is used when the default reseller prefix (`AUTH`) is not used. In practise this option is not set.
- The `match_fs_ignore_accounts` option allow to bypass some accounts from the process. In that case, the user and group of the file will be `root`.
