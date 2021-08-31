import com.sap.gateway.ip.core.customdev.util.Message;
import java.util.HashMap;
def Message processData(Message message) {
    //Body 
    def var1 = "[ ](?=[ ])|[\\,]|[^-_,A-Za-z0-9 ]+"
       def body = message.getBody(java.lang.String) as String;
       message.setBody(body.replaceAll(var1,""));

       return message;
}