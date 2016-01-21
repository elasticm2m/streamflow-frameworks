import groovy.json.*

def data = new JsonSlurper().parseText(body);

// manipulate data here

return JsonOutput.toJson(data)
