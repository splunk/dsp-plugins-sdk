# DSP Plugins SDK

An SDK for writing custom functions for the Splunk Data Stream Processor. Please refer to the [documentation](https://docs.splunk.com/Documentation/DSP/1.1.0/User/PluginSDK) for the most up to date information.

## Build Requirements

* Java 8

## Getting Started with Examples
* Clone this repository
* Update `gradle.properties`. Make sure the DSP API endpoint is accessible
* Run project setup using gradle:
```
# build dsp-plugin-examples module with example functions
$ ./gradlew dsp-plugin-examples:build

# register the plugin with DSP
$ ./gradlew registerPlugin -PSDK_PLUGIN_NAME=dsp-sdk-examples -PSDK_PLUGIN_DESC="Example SDK Functions"

# list all the plugins
$ ./gradlew getPlugins

# upload the jar built in the dsp-plugin-examples module to DSP
$ ./gradlew uploadPlugin -PPLUGIN_ID=<id> -PPLUGIN_MODULE=dsp-plugin-examples
```

Now the three functions `join_strings`, `map_expand` and `variable_write_log` in the example should be available in the DSP function registry and can be used to create pipelines.

Optionally, delete the example plugin:
```
$ ./gradlew deletePlugin -PPLUGIN_ID=<id>
```


## SDK API Documentation
All the Splunk jars dependent by this repo have their corresponding `*-javadoc.jar` available too. You can configure your IDE to download the `*-javadoc.jar` to view documentation.

If you use IntelliJ, the IDE will automatically download the `*-javadoc.jar`. You can view the javadoc following https://www.jetbrains.com/help/idea/viewing-reference-information.html#inline-quick-documentation
