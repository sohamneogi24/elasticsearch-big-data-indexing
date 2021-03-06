package com.neu.token;

import java.io.IOException;
import java.util.Date;

import javax.servlet.http.HttpServletRequest;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HomeController {

	@RequestMapping(value = "/generatetoken", method = RequestMethod.POST, consumes="application/json")
	public @ResponseBody String savePlan(HttpServletRequest request, @RequestBody String object) throws JSONException{
		final String secretKey = "deception";
		
		JSONObject jo = new JSONObject(object);
		Date expiry_date = new Date(System.currentTimeMillis()+10*60*1000);
		System.out.println(expiry_date.toString());
		jo.put("expiry", expiry_date );
		
		String originalString = jo.toString();
	    String encryptedString = AES.encrypt(originalString, secretKey) ;
	    //String decryptedString = AES.decrypt(encryptedString, secretKey) ;
	     
	    System.out.println(originalString);
	    System.out.println(encryptedString);
	    //System.out.println(decryptedString);
	    
	    return encryptedString;
		
	}
}
