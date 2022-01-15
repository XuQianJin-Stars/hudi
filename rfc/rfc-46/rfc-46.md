<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->
# RFC-[46]: Add RFC for Add Call Produce Command for Spark SQL


## Proposers
- @forwardxu

## Approvers
 - @vinoth @Danny @Raymond 

## Status

JIRA: [https://issues.apache.org/jira/browse/HUDI-3161](https://issues.apache.org/jira/browse/HUDI-3161)

## Abstract

We intend to implement Call Procedure Command for Spark SQL.

## Background
The Call Procedure Command can solve the problems that DDL and DML functions cannot handle. What I can think of are the following 4 aspects:
- Commit management
- Metadata table management
- Table migration
- Optimization table

## Implementation

### Extended SQL for CALL Command Syntax
#### Named arguments
All procedure arguments are named. When passing arguments by name, arguments can be in any order and any optional argument can be omitted.
```
CALL system.procedure_name(arg_name_2 => arg_2, arg_name_1 => arg_1)
```
#### Positional arguments
When passing arguments by position, the arguments may be omitted if they are optional.
```
CALL system.procedure_name(arg_1, arg_2, ... arg_n)
```
*note:* The system here has no practical meaning, the complete procedure name is system.procedure_name.

![](process.png)

As shown in the figure above, The execution process of Call Command consists of two parts, SQL Parser and Procedure Run.

### parse
In the sql parse stage, we will inject a HoodieSqlParser to spark sql which will parse our extended `CALL` syntax to LogicalPlan. If the HoodieSqlParser failed to parse the sql

statement, spark will rout it to spark’s sql parser. So we just need to implement our extended syntax in the HoodieSqlParser.

### resolution
In the resolution stage, some hoodie resolution rules will be injected to spark sql to resolve our extended LogicalPlan to the resolve plan which is a command plan for `CALL`.

### procedure#call
The Procedure#run method will translate the logical plan to hoodie’s api call. For example the `ShowCommitsProcedure` will translate to hoodie’s api for show commits to hoodie.

## Rollout/Adoption Plan

This is a new feature can use Spark SQL works with Spark-2.4.3+ & Spark 3.x versions.

## Test Plan

- [☑️] Unit tests for this feature
- [✖️] Product integration tests
- [✖️] Benchmark snapshot query for large tables
