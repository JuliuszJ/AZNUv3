package org.bp.travel;

import javax.xml.XMLConstants;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.xpath.XPathFactory;
import org.springframework.ws.soap.client.SoapFaultClientException;

public class BookingBusinessException extends RuntimeException {
    private final String service;
    private final Integer code;

    private BookingBusinessException(String service, Integer code, String message, Throwable cause) {
        super(message, cause);
        this.service = service;
        this.code = code;
    }

    public String getService() {
        return service;
    }

    public Integer getCode() {
        return code;
    }

    public static BookingBusinessException fromSoapFault(String service, SoapFaultClientException fault) {
        String message = fault.getFaultStringOrReason();
        Integer code = null;
        var detail = fault.getSoapFault().getFaultDetail();
        if (detail != null) {
            try {
                var factory = TransformerFactory.newInstance();
                factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
                factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
                factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_STYLESHEET, "");
                DOMResult result = new DOMResult();
                factory.newTransformer().transform(detail.getSource(), result);
                var xpath = XPathFactory.newInstance().newXPath();
                String text = xpath.evaluate("string(//*[local-name()='text'][1])", result.getNode());
                if (!text.isBlank()) {
                    message = text;
                }
                String value = xpath.evaluate("string(//*[local-name()='code'][1])", result.getNode());
                if (!value.isBlank()) {
                    code = Integer.valueOf(value.trim());
                }
            } catch (Exception parsingFailure) {
                // Keep the SOAP reason when optional business details cannot be read.
            }
        }
        return new BookingBusinessException(service, code, message, fault);
    }
}
