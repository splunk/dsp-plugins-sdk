# data-pipelines-plugin-template

A template used for DSP SDK plugins. This project will generate a skeleton plugin that can be further modified
to create a custom plugin that runs in a DSP pipeline.

## [Build Requirements](#buildRequirements)

* Java 8
* Network access to repo.splunk.com

## [Getting Started with Examples](#gettingStarted)
* Clone this repository
* Update `gradle.properties` (See [Gradle Properties](#gradleProperties)). Make sure the API endpoint is accessible
* Run project setup using gradle:
```
# build dsp-plugin-examples module with example functions
$ ./gradlew dsp-plugin-examples:build

# register a plugin with DSP
$ ./gradlew registerPlugin

# list all the plugins
$ ./gradlew getPlugins

# upload the jar built in dsp-plugin-examples module to DSP
$ ./gradlew uploadPlugin -PPLUGIN_ID=<id> -PPLUGIN_MODULE=dsp-plugin-examples
```

Now the three functions `join-strings`, `map-expand` and `variable-write-log` in the example should be available in DSP function registry and can be used to create pipelines.


## [Common Gradle Tasks](#commonTasks)
### Set up boiler code in dsp-plugin-functions module
```
./gradlew expandTemplates [-PSDK_FUNCTIONS_PATH=<path>]
```
By default, `expandTemplates` copies files from `templates` (set to `SDK_FUNCTIONS_PATH` in gradle.properties) to `dsp-plugin-functions` module.

Note that `expandTemplates` should generally only be run once. It can be run again, but will overwrite any existing files
with the same names.

### Build jars:

```
# this builds all modules in this repo
./gradlew build

# this builds only dsp-plugin-functions module
./gradlew dsp-plugin-functions:build
```

The plugin jar artifact will be found in `build/libs` in each module with name `<module>.jar`.

### Register a plugin with DSP
```
./gradlew registerPlugin [-PSDK_PLUGIN_NAME="sdk-examples" -PSDK_PLUGIN_DESC="Template SDK example functions."]
```

### List all plugins
```
./gradlew getPlugins
```

### Upload a jar to a plugin
```
./gradlew uploadPlugin -PPLUGIN_ID=<id> [-PPLUGIN_MODULE=<module>]
```
By default, `PLUGIN_MODULE` is set to `dsp-plugin-functions` in gradle.properties.

## [Gradle Properties Explained](#gradleProperties)
`SCLOUD_TOKEN_FILE` - Path to a text file containing only the value of the `access_token` field in the response from `scloud login`. This value should not have quotes around it.

`PLUGIN_UPLOAD_SERVICE_PROTOCOL` - `http` or `https`

`PLUGIN_UPLOAD_SERVICE_HOST` - Typically, the hostname used to reach the DSP API

`PLUGIN_UPLOAD_SERVICE_PORT` - Typically, the port used by the DSP API (ex. `443`)

`PLUGIN_UPLOAD_SERVICE_ENDPOINT` - Path to the DSP plugins endpoint (ex. `streams/v1/plugins`)
