import com.sap.gateway.ip.core.customdev.util.Message;
import java.util.HashMap;
import groovy.xml.*;

def Message processData(Message message) {
    def map = message.getProperties();
    def body = message.getBody(java.lang.String) as String;
    
    //Parse the xml
     def MessagLog = map.get("MessagLog")
     def ErrorInfo = "<ErrorInfo>"+body+"</ErrorInfo>"

  def response= new XmlSlurper().parseText(MessagLog)
  def newNode = new XmlSlurper().parseText(ErrorInfo)     
    
  response.root.EmpEmployment[0].appendNode(newNode);
  def outxml = groovy.xml.XmlUtil.serialize(response)
    
  message.setBody(outxml);           
  return message;
}