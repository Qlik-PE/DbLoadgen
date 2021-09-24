<#import "/spring.ftl" as spring />
<#import "lib/template.ftlh" as template>
<@template.header "Loadgen Home"/>

<#setting number_format="0">
<div>
    <div id="loadgenVersion">${buildProperties.getVersion()} [${buildProperties.getTime()}]</div>

    <h2>Loadgen H2 Section</h2>
    <p> this is a placeholder.</p>
</div>

<form id="delete-topic-form" action="<@spring.url '/test-connection'/>" method="POST">
    <button class="btn btn-danger" type="submit"><i class="fa fa-remove"></i>Test Connection</button>
    <input type="hidden" name="${_csrf.parameterName}" value="${_csrf.token}"/>
</form>

<div>
    <br/>
    <p>Message: ${message}</p>
</div>

<@template.footer/>

