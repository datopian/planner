# Planner

[![Travis](https://img.shields.io/travis/datahq/planner/master.svg)](https://travis-ci.org/datahq/planner)
[![Coveralls](http://img.shields.io/coveralls/datahq/planner.svg?branch=master)](https://coveralls.io/r/datahq/planner?branch=master)

Create flows out of specs

## Flow Specs

```yaml
meta:
  owner: <owner username>
  ownerid: <owner unique id>
  dataset: <dataset name>
  version: 1
  findability: <published/unlisted/private>
inputs:
 -  # only one input is supported atm
    kind: datapackage
    parameters:
      descriptor: <datapackage-descriptor>
      resource-mapping:
        <resource-name-or-path>: <resource-url>
        
processing:
 - # Processing steps that need to be done on sources to get proper data streams
   input: <source-resource-name - e.g. `my-excel-resource`>
   output: <destination-resource-name - e.g. `my-excel-resource-sheet-1`>
   tabulator: # Currently we're only supporting tabulator transformations
     # These are sample options for tabulator, see its docs for all available options
     headers: 2 
     sheet: 1

outputs:
  -
    kind: rdbms  # dump.to_sql
    parameters:
      engine: <tbd, should be the name of a user provided configuration - not the actual connection string>
  -
    kind: sqlite # dump.to_sql
  -
    kind: npm  
    parameters:
      credentials: <tbd, should be the name of a user provided configuration - not the actual credentials>
  -
    kind: zip (dump.to_zip)
    parameters: 
        out-file: <name of the file>
  - ... # other output formats

```


## Processing Flows
Processing flows are part of the 'assembly line' for a single assembled dataset.
All processor flows are single pipelines, and generate one or more _processing artifacts_.
These artifacts are later collected to create the final datapackage.

By default, we add 3 processing flows:
- Convert each tabular resource to CSV
- Convert each tabular resource to JSON
- Copy non-tabular resources from source to target.

### Implementing a new processing flow

Usually when adding a new output format or transformation we will add a new flow for that.

In order to create that, we need to implement a class inheriting from `BaseProcessingNode`,
and add that class name to the `ORDERED_NODE_CLASSES` array (in the correct location).

Each of these classes is provided with some processing artifacts and it must return the new
processing artifact(s) that it is knows how to generate.
The `ProcessingArtifact` class represents a single such artifact. It holds:
- The name of the resource that will contain the artifact
- The datahub type of that artifact
- Its dependencies:
  - Which artifacts are required to be streaming in the pipeline prior to processing
  - Which artifacts need to be present in the datapackage as non-streaming resources
- The datapackage-pipelines steps that will generate the artifact and create that resource
  the datapackage.

### Under the hood
Each of these artifacts is converted into a pipeline, which always has the following structure:
  - Create a new, empty datapackage
  - Load all the 'streaming' dependent resources and stream them into the pipeline
  - Add any other dependent resources without streaming them into the pipeline
  - Add the specific steps that were introduced by the specific flow
  - Add a 'dump to target' step to save the intermediate resources into storage (S3)

The pipelines' execution order is maintained by properly setting the dependencies between the
 different pipelines. This is based on the list of required artifacts that each flow
  defines.

An aggregating pipeline is also created that assembles the final package, based on all the
  separate intermediate packages. This aggregating datapackage does not need to process any
  data, as it copies resources with absolute URL paths (and not relative ones).

## Contributing

Please read the contribution guideline:

[How to Contribute](CONTRIBUTING.md)

Thanks!
