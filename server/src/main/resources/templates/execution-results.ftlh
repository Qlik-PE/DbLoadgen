<#import "/spring.ftl" as spring />
<#import "lib/template.ftlh" as template>
<@template.header "Execution Results"/>

<#setting number_format="0">

<#if preload == true >
    <#assign refresh = true >
    <#assign refreshFreq = 5000 >
    <#assign executionStatus = "Preload phase is running ..." >
    <#assign executionPct = preloadPct >
<#elseif cdcTest == true>
    <#assign refresh = true >
    <#assign refreshFreq = 5000 >
    <#assign executionStatus = "CDC phase is running ..." >
    <#assign executionPct = cdcTestPct >
<#elseif parsingMetadata == true>
    <#assign refresh = true >
    <#assign refreshFreq = 1000 >
    <#assign executionStatus = "Parsing table metadata ..." >
    <#assign executionPct = parsingPct >
<#elseif initializingSchema == true>
    <#assign refresh = true >
    <#assign refreshFreq = 1000 >
    <#assign executionStatus = "Initializing Schema ..." >
    <#assign executionPct = schemaPct >
<#elseif executingCommand == true>
    <#assign refresh = true >
    <#assign refreshFreq = 1000 >
    <#assign executionStatus = "Execution Status ..." >
    <#assign executionPct = 50 >
<#else>
    <#assign refresh = false >
    <#assign refreshFreq = 100 >
    <#assign executionStatus = "Execution is complete ..." >
    <#assign executionPct = "100" >
</#if>
<#if refresh == true>
    <script type="text/javascript">
        setTimeout(function(){
            window.location.reload(true);
        }, ${refreshFreq});
    </script>
</#if>

<div>
    <h2>Execution Results</h2>

    <div id="status" >
        <table class="table-condensed" style="border-collapse: collapse;">
            <tr>
                <td style="width: 300px">
                    <h5 class="text-success">${executionStatus}</h5>
                </td>
                <td>
                    <div class="progress" style="width: 500px;height: 20px;font-size: .8rem;font-weight: bold;">
                        <div class="progress-bar bg-success text-dark" role="progressbar" style="height: 20px; width: ${executionPct}%;" aria-valuenow="${executionPct}" aria-valuemin="0" aria-valuemax="100">
                            ${executionPct}%
                        </div>
                    </div>

                </td>
            </tr>
        </table>
    </div>

    <#if results??>
        <br/>
        <div id="results">
            <@results?interpret />
        </div>
    </#if>
    <#if preloadResults??>
        <br/>
        <div id="preloadResults">
            <@preloadResults?interpret />
        </div>
    </#if>
    <#if cdcResults??>
        <br/>
        <div id="cdcResults">
            <@cdcResults?interpret />
        </div>
    </#if>

    <br/>
    <#if refresh == false >
      <form id="ok" action="<@spring.url '/'/>" method="GET">
            <button class="btn btn-success" type="submit"><i class="fa fa-check"></i>&nbsp;&nbsp;OK</button>
            <input type="hidden" name="${_csrf.parameterName}" value="${_csrf.token}"/>
        </form>
    <#else>
        <form id="ok" action="<@spring.url '/stop-execution'/>" method="POST">
            <button class="btn btn-danger" type="submit"><i class="fa fa-ban"></i>&nbsp;&nbsp;Stop Execution</button>
            <input type="hidden" name="${_csrf.parameterName}" value="${_csrf.token}"/>
        </form>
    </#if>

</div>

<@template.footer/>

