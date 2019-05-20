# Mercury SDK
The mercury sdk contains a python library used for interacting with
the Mercury HTTP API service as well as the Mercury CLI, `mcli`. 


## Installation
    $ pip install mercury-sdk==0.0.14.dev1
    
## MCLI
### Configuration
    $ mkdir -p ~/.mercury-sdk

#### ~/.mercury_sdk/mcli.yml
    mercury_api:
        url: <MERCURY_API_URL>

#### Authentication with Keystone
mcli current provides a keystonev2 frontend. If your API is provisioned
behind a repose proxy with keystone integration you may add something
similar to your `mcli.yml`

    auth_handler: keystone

    auth:
      username: <USERNAME>
      password: <PASSWORD>
      url: <IDENTITY_URL>
      type: password

### Usage
```
$ mcli --help
usage: mcli [-h] [--version] [-c CONFIG_FILE]
            [--program-directory PROGRAM_DIRECTORY]
            [--token-cache TOKEN_CACHE] [-m MERCURY_URL] [-v] [--no-auth]
            {login,logout,set-token,inventory,rpc,shell,press,deploy,ansible}
            ...

The Mercury Command Line Interface

positional arguments:
  {login,logout,set-token,inventory,rpc,shell,press,deploy,ansible}
                        <command> --help
    login               Login to the authentication service and store the
                        token in the local environment
    logout              Logout of the authentication service
    set-token           Bypass auth handlers and set a token directly
    inventory           Inventory query operations
    rpc                 RPC commands
    shell               Enter a shell for interactive inventory management
    press               Install an operating system to /mnt/press
    deploy              [EXPERIMENTAL] Deploy many servers at once using an
                        asset file
    ansible             Helper commands for ansible

optional arguments:
  -h, --help            show this help message and exit
  --version             Display the program version
  -c CONFIG_FILE, --config-file CONFIG_FILE
                        SDK configuration file
  --program-directory PROGRAM_DIRECTORY
                        Alternative location for program data
  --token-cache TOKEN_CACHE
                        alternative location of the token cache
  -m MERCURY_URL, --mercury-url MERCURY_URL
                        The mercury url to use
  -v                    Verbosity level -v, somewhat verbose, -vvv really
                        verbose
  --no-auth             Skip authentication

SDK version 0.0.14.dev1
```

### Querying the inventory
The inventory sub-command provides a nice interface for extracting 
data from the mercury inventory
```
$ mcli inventory --help
usage: mcli inventory [-h] [-q QUERY] [-p PROJECTION] [-n MAX_ITEMS] [-a]
                      [mercury_id]

positional arguments:
  mercury_id            Get a device record by mercury_id

optional arguments:
  -h, --help            show this help message and exit
  -q QUERY, --query QUERY
                        A mercury query to execute in valid JSON. Use "-" and
                        the value will be read from stdout use "@filename" and
                        the query will be read from this file
  -p PROJECTION, --projection PROJECTION
                        Specify the key projection to produce the desired
                        output
  -n MAX_ITEMS, --max-items MAX_ITEMS
  -a, --active          Only search for active devices
```

#### Basic usage
Get 5 items from the inventory and display their product type
```
$ mcli inventory -n5 -pdmi.product_name
{
  "items": [
    {
      "_id": "5a581c44fc6db1767b6e7e30",
      "dmi": {
        "product_name": "PowerEdge R720"
      },
      "mercury_id": "01bfa3c31ec6f2074fa83100d1087f66970fcae742"
    },
    {
      "_id": "5a581b21fc6db1767b6e7e2f",
      "dmi": {
        "product_name": "ProLiant DL380 Gen9"
      },
      "mercury_id": "01b4041c2841ea65b12e1db4907eb359f40be10c84"
    },
    {
      "_id": "5a581b21fc6db1767b6e7e2e",
      "dmi": {
        "product_name": "ProLiant DL380 Gen9"
      },
      "mercury_id": "01671a90e1354f15263c77d695a194da49095ffe06"
    },
    {
      "_id": "5a581dd3fc6db1767b6e7e31",
      "dmi": {
        "product_name": "ProLiant DL380 Gen9"
      },
      "mercury_id": "010f9e1b042d98943c19240e16a959a94b1dc5dfb7"
    },
    {
      "_id": "5a582f16fc6db1767b6e7e33",
      "dmi": {
        "product_name": "ProLiant DL380 Gen9"
      },
      "mercury_id": "01e516e9f6260e28922c8895c0fdf5fad2be89627f"
    }
  ],
  "limit": 5,
  "sort_direction": "ASCENDING",
  "total": 19859
}
```

Add an inventory query to isolate a specific set of devices

```
$ mcli inventory --query='
  {"origin.datacenter": "ord1",
   "interfaces.lldp.switch_name": {
    "$regex": "c10-5.*"}}' \
    -a -n1 \
    --projection=raid.total_drives,interfaces.lldp,os_storage.media_type,os_storage.size,os_storage.model,os_storage.device

{
  "items": [
    {
      "_id": "5a70c463fc6db12b334cf3e8",
      "interfaces": [
        {
          "lldp": {
            "port_number": 17,
            "switch_name": "c10-5-1e.ord1"
          }
        },
        {
          "lldp": {
            "port_number": 40,
            "switch_name": "c10-5-2e.ord1"
          }
        },
        {
          "lldp": {
            "port_number": 40,
            "switch_name": "c10-5-1e.ord1"
          }
        },
        {
          "lldp": {
            "port_number": 17,
            "switch_name": "c10-5-2e.ord1"
          }
        }
      ],
      "mercury_id": "01a30e24ca5be2a482a7edc6660ba1c8320518a1e5",
      "os_storage": [
        {
          "device": "/dev/sda",
          "media_type": "disk",
          "model": "HP LOGICAL VOLUME (scsi)",
          "size": 299966445568
        },
        {
          "device": "/dev/sdb",
          "media_type": "ssd",
          "model": "ATA MK000960GWCFA (scsi)",
          "size": 960197124096
        },
        {
          "device": "/dev/sdc",
          "media_type": "ssd",
          "model": "ATA MK000960GWCFA (scsi)",
          "size": 960197124096
        },
        {
          "device": "/dev/sdd",
          "media_type": "ssd",
          "model": "ATA MK000480GWEZH (scsi)",
          "size": 480103981056
        },
        {
          "device": "/dev/sde",
          "media_type": "ssd",
          "model": "ATA MK000480GWEZH (scsi)",
          "size": 480103981056
        }
      ],
      "raid": [
        {
          "total_drives": 2
        },
        {
          "total_drives": 0
        }
      ]
    }
  ],
  "limit": 1,
  "sort_direction": "ASCENDING",
  "total": 19
}
```


## RPC

Mercury provides an RPC system for interacting with devices running an active agent.

### Discovering active devices

```
└─ $ ▶ mcli inventory --active -n1
{
  "items": [
    {
      "_id": "5a5e2624fc6db1767b6e7f50",
      "mercury_id": "012fb22e5f882fc5f3b0e114d8b8719a225f807992"
    }
  ],
  "limit": 1,
  "sort_direction": "ASCENDING",
  "total": 532
}
```

### Enumerating capabilities
Many rpc commands are common, but a few are chassis specific. To view a devices 
supported capabilities use the `rpc list` command

```bash
$ mcli rpc list 012fb22e5f882fc5f3b0e114d8b8719a225f807992
``` 

### Running a command

```bash
$ mcli rpc submit --method echo --args "Hello World" --target 012fb22e5f882fc5f3b0e114d8b8719a225f807992 --wait
```
It is also possible to run a command against multiple computers using a query
```bash
$ mcli rpc subbmit --method run --args "uptime" --query '{"dmi.sys_vendor": "HP"}' --wait 
```

### Entering a mercury shell
```
$ mcli shell --query '{"pci.device_name": "GP100GL [Tesla P100 PCIe 12GB]"}"}'
```

### Running adhoc commands
```bash
$ mcli shell --query '{"bmc.network.ip_address": "10.10.2.9"}' --run "dmesg| grep -i error"
```


