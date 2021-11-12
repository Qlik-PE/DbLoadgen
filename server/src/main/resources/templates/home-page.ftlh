<#import "/spring.ftl" as spring />
<#import "lib/template.ftlh" as template>
<@template.header "DbLoadgen Job Execution"/>

<#setting number_format="0">
<div>
    <div id="dbloadgenVersion">${buildProperties.getVersion()} [${buildProperties.getTime()}]</div>

    <form id="run-test" action="<@spring.url '/execute-command'/>" method="POST">

        <script type="text/javascript">
            function reloadPage(name, value) {
                window.location = window.location.pathname + "?" + name + "=" + escape(value);
            }
        </script>

        <table>
            <tbody>
            <tr>
                <td>
                    <h5><i class="fa fa-connectdevelop"></i>&nbsp;&nbsp;Connection Info</h5>
                </td>
                <td style="width: 5%">&nbsp;</td>
                <td>
                    <h5><i class="fa fa-building-o"></i>&nbsp;&nbsp;Workload Configuration</h5>
                </td>
            </tr>
            <tr>
                <td style="vertical-align: top">
                    <table class="table table-bordered">
                        <tbody>
                        <tr>
                            <td>
                                <label for="connectionName"><i class="fa fa-id-badge"></i>&nbsp;&nbsp;Connection Name</label>
                            </td>
                            <td>
                                <select id="connectionName" name="connectionName"
                                        onchange="reloadPage(this.name, this.options[this.selectedIndex].value)">
                                    <#list connectionNames as connectionName>
                                        <#if connectionName == connection.getName()>
                                            <#assign selected = "selected">
                                        <#else>
                                            <#assign selected = "">
                                        </#if>
                                        <option value="${connectionName}" ${selected}>${connectionName}</option>
                                    </#list>
                                </select>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <label for="databaseType"><i class="fa fa-database"></i>&nbsp;&nbsp;Database Type</label>
                            </td>
                            <td>
                                <input type="text" id="databaseType" name="databaseType"
                                       value="${connection.getDatabaseType()}" readonly>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <label for="jdbcDriver"><i class="fa fa-automobile"></i>&nbsp;&nbsp;JDBC Driver Name</label>
                            </td>
                            <td>
                                <input type="text" id="jdbcDriver" name="jdbcDriver" size="40"
                                       value="${connection.getJdbcDriver()}" readonly>
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <label for="connectionUrl"><i class="fa fa-globe"></i>&nbsp;&nbsp;JDBC URL</label>
                            </td>
                            <td>
                                <input type="text" id="connectionUrl" name="connectionUrl" size="40"
                                       value="${connection.getUrl()}">
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <label for="userName"><i class="fa fa-user"></i>&nbsp;&nbsp;User Name</label>
                            </td>
                            <td>
                                <input type="text" id="userName" name="userName" value="${connection.getUsername()}">
                            </td>
                        </tr>
                        <tr>
                            <td>
                                <label for="password"><i class="fa fa-key"></i>&nbsp;&nbsp;Password</label>
                            </td>
                            <td>
                                <input type="text" id="password" name="password" value="${connection.getPassword()}">
                            </td>
                        </tr>
                        </tbody>
                    </table>
                </td>
                <td>&nbsp;</td>

                <td style="vertical-align: top" rowspan="2">
                    <table class="table table-bordered">
                        <tbody>
                        <tr>
                            <td><label for="workloadName"><i class="fa fa-id-badge"></i>&nbsp;&nbsp;Workload
                                    Name</label></td>
                            <td>
                              <select id="workloadName" name="workloadName"
                                      onchange="reloadPage(this.name, this.options[this.selectedIndex].value)">
                                  <#list workloadNames as workloadName>
                                      <#if workloadName == workload.getName()>
                                          <#assign selected = "selected">
                                      <#else>
                                          <#assign selected = "">
                                      </#if>
                                      <option value="${workloadName}" ${selected}>${workloadName}</option>
                                  </#list>
                              </select>
                            </td>
                        </tr>
                        <tr>
                            <td><label for="schema"><i class="fa fa-database"></i>&nbsp;&nbsp;Schema Name</label></td>
                            <td><input type="text" id="schema" name="schema" value="${workload.getSchema()}"></td>
                        </tr>
                        <tr>
                            <td><label for="preloadThreads"><i class="fa fa-briefcase"></i>&nbsp;&nbsp;Preload
                                    Threads</label></td>
                            <td><input type="text" id="preloadThreads" name="preloadThreads"
                                       value="${workload.getPreloadThreads()}"></td>
                        </tr>
                        <tr>
                            <td><label for="preloadBatchSize"><i class="fa fa-cogs"></i>&nbsp;&nbsp;CDC Batch
                                    Size</label></td>
                            <td><input type="text" id="preloadBatchSize" name="preloadBatchSize"
                                       value="${workload.getPreloadBatchSize()}"></td>
                        </tr>
                        <tr>
                            <td><label for="duration"><i class="fa fa-clock-o"></i>&nbsp;&nbsp;CDC Duration
                                    (minutes)</label></td>
                            <td><input type="text" id="duration" name="duration" value="${workload.getDuration()}"></td>
                        </tr>
                        <tr>
                            <td><label for="workerThreads"><i class="fa fa-briefcase"></i>&nbsp;&nbsp;CDC
                                    Threads</label></td>
                            <td><input type="text" id="workerThreads" name="workerThreads"
                                       value="${workload.getWorkerThreads()}"></td>
                        </tr>
                        <tr>
                            <td><label for="workerThreadSleep"><i class="fa fa-bed"></i>&nbsp;&nbsp;CDC Thread Sleep
                                    (milliseconds)</label></td>
                            <td><input type="text" id="workerThreadSleep" name="workerThreadSleep"
                                       value="${workload.getWorkerThreadSleep()}"></td>
                        </tr>
                        <tr>
                            <td><label for="workerBatchSize"><i class="fa fa-cogs"></i>&nbsp;&nbsp;CDC Batch
                                    Size</label></td>
                            <td><input type="text" id="workerBatchSize" name="workerBatchSize"
                                       value="${workload.getWorkerBatchSize()}"></td>
                        </tr>
                        <tr>
                            <td><label for="workerBatchSleep"><i class="fa fa-bed"></i>&nbsp;&nbsp;CDC Batch Sleep
                                    (milliseconds)</label></td>
                            <td><input type="text" id="workerBatchSleep" name="workerBatchSleep"
                                       value="${workload.getWorkerBatchSleep()}"></td>
                        </tr>
                        <tr>
                            <td><label for="allTablesAllThreads"><i class="fa fa-arrows"></i>&nbsp;&nbsp;Modify All
                                    Tables in All Worker Threads</label></td>
                            <td><input type="text" id="allTablesAllThreads" name="allTablesAllThreads"
                                       value="${workload.getAllTablesAllThreads()}"></td>
                        </tr>
                        <tr>
                            <td><i class="fa fa-list"></i>&nbsp;&nbsp;CDC Operation Distribution (%)</td>
                            <td>
                                <label for="insertPct">Insert: </label>
                                <input type="text" id="insertPct" name="insertPct" size=4
                                       value="${workload.getOperationPct().getInsert()}">
                                &nbsp;&nbsp;&nbsp;
                                <label for="updatePct">Update: </label>
                                <input type="text" id="updatePct" name="updatePct" size=4
                                       value="${workload.getOperationPct().getUpdate()}">
                                &nbsp;&nbsp;&nbsp;
                                <label for="deletePct">Delete: </label>
                                <input type="text" id="deletePct" name="deletePct" size=4
                                       value="${workload.getOperationPct().getDelete()}">
                            </td>
                        </tr>
                        </tbody>
                    </table>
                </td>
            </tr>
            <tr>
                <td>
                    <fieldset class="form-group">
                        <legend class="mt-4">Command Options</legend>
                        <div class="form-check">
                            <label class="form-check-label">
                                <input type="radio" class="form-check-input" name="command" id="test-connection"
                                       value="test-connection" checked="">
                                Test database connectivity
                            </label>
                        </div>
                        <div class="form-check">
                            <label class="form-check-label">
                                <input type="radio" class="form-check-input" name="command" id="init" value="init">
                                Init: Create the schema and tables for this workload
                            </label>
                        </div>
                        <div class="form-check">
                            <label class="form-check-label">
                                <input type="radio" class="form-check-input" name="command" id="preload" value="preload">
                                Preload the tables for this workload
                            </label>
                        </div>
                        <div class="form-check">
                            <label class="form-check-label">
                                <input type="radio" class="form-check-input" name="command" id="run" value="run">
                                Run the CDC workload against the database
                            </label>
                        </div>
                        <div class="form-check">
                            <label class="form-check-label">
                                <input type="radio" class="form-check-input" name="command" id="cleanup" value="cleanup">
                                Cleanup: Remove the tables and schema associated with this workload
                            </label>
                        </div>
                        <br/>
                        <div class="form-check">
                            <label class="form-check-label">
                                <input type="radio" class="form-check-input" name="command" id="reset" value="reset">
                                Reset the environment: cleanup, init, and preload
                            </label>
                        </div>
                        <div class="form-check">
                            <label class="form-check-label">
                                <input type="radio" class="form-check-input" name="command" id="end-to-end"
                                       value="end-to-end">
                                Run an end-to-end test: cleanup, init, preload, and run
                            </label>
                        </div>
                    </fieldset>

                </td>
            </tr>
            </tbody>
        </table>
        <br/>
        <button class="btn btn-primary" type="submit"><i class="fa fa-plus"></i>&nbsp;&nbsp;Execute</button>
        <input type="hidden" name="${_csrf.parameterName}" value="${_csrf.token}"/>
    </form>

</div>

<@template.footer/>

