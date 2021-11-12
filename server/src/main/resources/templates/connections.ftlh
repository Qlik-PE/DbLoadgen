<#import "/spring.ftl" as spring />
<#import "lib/template.ftlh" as template>
<@template.header "DbLoadgen Connections"/>

<#setting number_format="0">
<div>
    <div id="dbloadgenVersion">${buildProperties.getVersion()} [${buildProperties.getTime()}]</div>

</div>

<!-- Button to Open the Connection Modal -->
<button type="button" class="btn btn-primary" data-toggle="modal" data-target="#connectionModal">
    Choose a Connection
</button>
<div class="modal" id="connectionModal">
    <div class="modal-dialog" >
        <div class="modal-content">
            <div class="modal-header">
                <h5 class="modal-title">Choose a Connection</h5>
                <button type="button" class="btn-close" data-dismiss="modal" aria-label="Close">
                </button>
            </div>
            <div class="modal-body">
                <p>Select connection body text goes here.</p>
                <button class="btn btn-primary" type="button"><i class="fa fa-plus"></i>Test Connection</button>
            </div>
            <div class="modal-footer">
                <button type="button" class="btn btn-primary">Save changes</button>
                <button type="button" class="btn btn-secondary" data-dismiss="modal">Close</button>
            </div>
        </div>
    </div>
</div>
<div>
<form id="connection-form" action="<@spring.url '/select-connection'/>" method="POST">
    <button class="btn btn-primary" type="submit"><i class="fa fa-plus"></i>Test Connection</button>
    <input type="hidden" name="${_csrf.parameterName}" value="${_csrf.token}"/>
</form>
</div>

<div>
    <br/>
    <p>Message: ${message}</p>
</div>

<@template.footer/>

