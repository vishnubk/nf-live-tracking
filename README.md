```markdown
# nf-live-tracking

The `nf-live-tracking` plugin provides live tracking and status updates for each Nextflow process from the submission stage to completion or failure. It outputs a JSON file containing the values of input and output variables involved in each process. This plugin is particularly useful for building analytics, integrating with Kafka, or syncing with your database systems.

## Getting Started

The `nf-live-tracking` plugin requires Nextflow version `23.04.0` or later.

To enable and configure `nf-live-tracking`, add the following snippet to your Nextflow config file and adjust as necessary:

```groovy
plugins {
  id 'nf-live-tracking@2.0.0'
}

prov {
  enabled = true
  formats {
    live {
      file = "workflowTrace.json"
      overwrite = true
    }
  }
}
```
Currently, this plugin is not part of the nextflow plug-in registry. If you would like to use it, clone this repo and run make compile and make install. Have a look at the `workflowTrace.json` to see if such outputs are useful for you.

Execute your Nextflow pipeline as usual. The `nf-live-tracking` plugin will automatically generate the specified JSON file without requiring any modifications to your pipeline script.

## Development

To build and test the `nf-live-tracking` Nextflow plugin, execute the following commands. Refer to the [nf-hello](https://github.com/nextflow-io/nf-hello) README for additional instructions (e.g., publishing the plugin).

```bash
# (Optional) Check out the relevant feature branch
# git checkout <branch>

# Create an empty directory for both nf-live-tracking and nextflow repositories
git clone --depth 1 -b STABLE-23.10.x https://github.com/nextflow-io/nextflow ../nextflow

# Set up the nextflow repository
cd ../nextflow && ./gradlew compile exportClasspath && cd -

# Set up the nf-live-tracking repository
grep -v 'includeBuild' settings.gradle > settings.gradle.bkp
echo "includeBuild('../nextflow')" >> settings.gradle.bkp
mv -f settings.gradle.bkp settings.gradle
./gradlew assemble

# Launch
./launch.sh run test.nf -plugins nf-live-tracking
```

