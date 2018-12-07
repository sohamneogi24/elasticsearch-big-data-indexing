package com.abds.prototype1;

import org.springframework.web.bind.annotation.RestController;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.load.SchemaLoader;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;

import redis.clients.jedis.Jedis;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.DigestUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

@RestController
public class MainController {

	@Autowired
	Jedis jedis;

	Map<String, String> planMap = new HashMap<String, String>();
	Map<String, String> planCostShareMap = new HashMap<String, String>();
	Map<String, String> linkedPlanServices = new HashMap<String, String>();
	Map<String, String> linkedService = new HashMap<String, String>();
	Map<String, String> planserviceCostShares = new HashMap<String, String>();

	@RequestMapping(value = "/plan", method = RequestMethod.POST)
	public @ResponseBody String savePlan(HttpServletRequest request, HttpServletResponse response,
			@RequestBody String object) throws JSONException, ProcessingException, IOException {
		if (request.getHeader("Authorization") == null) {
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
			return "Please provide authorization token";

		}
		if (!tokenValidation(request)) {
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
			return "Unauthorised";
		}

		QueueAdapter qa = new QueueAdapter(jedis);

		JSONObject planJsonObj = new JSONObject(object);
		String inputstream = planJsonObj.toString();
		InputStream is = new ByteArrayInputStream(inputstream.getBytes());
		String etag = generateETagHeaderValue(is);
		
		
		InputStream inputStream = getClass().getResourceAsStream("/json_schema.json");
		JSONObject rawSchema = new JSONObject(new JSONTokener(inputStream));
		String schema = rawSchema.toString();
		
//		Schema schema = SchemaLoader.load(rawSchema);
//		schema.validate(obj); 
		

		if (ValidationUtils.isJsonValid(schema, object)) {
			Iterator<?> keys = planJsonObj.keys();
			String key = "";
			while (keys.hasNext()) {
				key = (String) keys.next();
				if (planJsonObj.get(key) instanceof JSONObject) {
					JSONObject planCostShareJsonObj = (JSONObject) planJsonObj.get(key);
					planCostShareMap = toMap(planCostShareJsonObj);
					save(planJsonObj.getString("objectType") + "_" + planJsonObj.getString("objectId") + "_" + key,
							planCostShareMap);
					qa.sendJobToWaitQueue(
							planJsonObj.getString("objectType") + "_" + planJsonObj.getString("objectId") + "_" + key);
				} else if (planJsonObj.get(key) instanceof JSONArray) {
					JSONArray linkedPlanServicesJsonArray = (JSONArray) planJsonObj.get(key);
					for (int i = 0; i < linkedPlanServicesJsonArray.length(); i++) {
						JSONObject linkedServiceJsonObj = (JSONObject) linkedPlanServicesJsonArray.get(i);
						Iterator<?> linkedServiceKeys = linkedServiceJsonObj.keys();
						String linkedServiceKey = "";
						while (linkedServiceKeys.hasNext()) {
							linkedServiceKey = (String) linkedServiceKeys.next();
							if (linkedServiceJsonObj.get(linkedServiceKey) instanceof JSONObject) {
								JSONObject obj1 = (JSONObject) linkedServiceJsonObj.get(linkedServiceKey);
								linkedService = toMap(obj1);
								save(planJsonObj.getString("objectType") + "_" + planJsonObj.getString("objectId") + "_"
										+ key + "_" + linkedServiceJsonObj.getString("objectId") + "_"
										+ linkedServiceKey, linkedService);
								qa.sendJobToWaitQueue(planJsonObj.getString("objectType") + "_"
										+ planJsonObj.getString("objectId") + "_" + key + "_"
										+ linkedServiceJsonObj.getString("objectId") + "_" + linkedServiceKey);
								linkedService.clear();
							} else {
								linkedPlanServices.put(linkedServiceKey,
										String.valueOf(linkedServiceJsonObj.getString(linkedServiceKey)));
							}
						}
						save(planJsonObj.getString("objectType") + "_" + planJsonObj.getString("objectId") + "_" + key
								+ "_" + linkedServiceJsonObj.getString("objectId") + "_"
								+ linkedServiceJsonObj.getString("objectType"), linkedPlanServices);
						qa.sendJobToWaitQueue(
								planJsonObj.getString("objectType") + "_" + planJsonObj.getString("objectId") + "_"
										+ key + "_" + linkedServiceJsonObj.getString("objectId") + "_"
										+ linkedServiceJsonObj.getString("objectType"));
						String sKey = planJsonObj.getString("objectType") + "_" + planJsonObj.getString("objectId")
								+ "_" + key;
						jedis.sadd(sKey,
								planJsonObj.getString("objectType") + "_" + planJsonObj.getString("objectId") + "_"
										+ key + "_" + linkedServiceJsonObj.getString("objectId") + "_"
										+ linkedServiceJsonObj.getString("objectType"));
					}
				} else {
					planMap.put(key, String.valueOf(planJsonObj.get(key)));
				}
			}
			planMap.put("etag", etag);
			save(planJsonObj.getString("objectType") + "_" + planJsonObj.getString("objectId"), planMap);
			qa.sendJobToWaitQueue(planJsonObj.getString("objectType") + "_" + planJsonObj.getString("objectId"));
		} else {
			return "Invalid JSON";
		}
		response.setHeader("Etag", etag);
		return planJsonObj.getString("objectId");
	}

	public void save(String key, Map<String, String> planCostShareMap2) {
		jedis.hmset(key, planCostShareMap2);
	}

	@ResponseBody
	@RequestMapping(value = "/plan/{id}", method = RequestMethod.GET, produces = "application/json")
	public String getPlans(HttpServletRequest request, HttpServletResponse response, @PathVariable String id)
			throws JSONException, IOException {
		JSONObject object = new JSONObject();
		if (request.getHeader("Authorization") == null) {
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
			object.put("Error", "Please provide authorization token");
			return object.toString();
		}
		if (!tokenValidation(request)) {
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
			object.put("Error", "Unauthorized");
			return object.toString();
		}

		Map<String, String> plan = jedis.hgetAll("plan_" + id);
		if (!plan.isEmpty()) {
			String headerETag = request.getHeader("If-None-Match");
			String storedEtag = plan.get("etag");

			if (headerETag == null || storedEtag == null || !headerETag.equals(storedEtag)) {

				response.setHeader("Etag", storedEtag);
				Iterator it = plan.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pairs = (Map.Entry) it.next();
					object.put((String) pairs.getKey(), pairs.getValue());
				}
				Map<String, String> planCostShare = jedis.hgetAll("plan_" + id + "_planCostShares");
				object.put("planCostShares", planCostShare);
				Set<String> setLinkedPlans = jedis.smembers("plan_" + id + "_linkedPlanServices");
				JSONArray linkedpricearray = new JSONArray();
				Iterator<String> iter = setLinkedPlans.iterator();
				while (iter.hasNext()) {
					String aaa = iter.next();
					Map<String, String> linkedPlanServices = jedis.hgetAll(aaa);
					JSONObject object1 = new JSONObject();
					Iterator iter1 = linkedPlanServices.entrySet().iterator();
					String idkey = "";
					while (iter1.hasNext()) {
						Map.Entry pairs = (Map.Entry) iter1.next();
						if (pairs.getKey().equals("objectId")) {
							idkey = (String) pairs.getValue();

						}
						object1.put((String) pairs.getKey(), pairs.getValue());
					}
					String key_1 = "plan_" + id + "_linkedPlanServices_" + idkey + "_planserviceCostShares";
					String key_2 = "plan_" + id + "_linkedPlanServices_" + idkey + "_linkedService";
					Map<String, String> general1 = jedis.hgetAll(key_1);
					Map<String, String> general2 = jedis.hgetAll(key_2);
					object1.put("planserviceCostShares", general1);
					object1.put("linkedService", general2);
					linkedpricearray.put(object1);
				}
				object.put("linkedPlanServices", linkedpricearray);
				return object.toString();
			} else {
				response.setStatus(HttpServletResponse.SC_NOT_MODIFIED);
				object.put("Message", "Plan not modified, valid etag");
				return object.toString();
			}
		} else {
			response.setStatus(HttpServletResponse.SC_NOT_FOUND);
			object.put("Message", "Plan Does not exist");
			return object.toString();
		}

	}

	@RequestMapping(value = "/plan/{id}", method = RequestMethod.DELETE)
	public @ResponseBody String deletePlan(HttpServletRequest request, HttpServletResponse response,
			@PathVariable String id) throws JSONException {
		if (request.getHeader("Authorization") == null) {
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
			return "Please provide authorization token";
		}

		if (!tokenValidation(request)) {
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
			return "Unauthorised";
		}
		Set<String> keys = jedis.keys("plan_" + id + "*");
		Set<String> linkedPlanServicesKeys = new HashSet<String>();
		for (String key : keys) {
			String o = "plan_" + id + "_linkedPlanServices";
			if (key.equals(o)) {
				linkedPlanServicesKeys = jedis.smembers(o);
				for (String k : linkedPlanServicesKeys) {
					linkedPlanServicesKeys.add(k);
				}
			}
		}
		if (!keys.isEmpty()) {
			jedis.del(linkedPlanServicesKeys.toArray(new String[linkedPlanServicesKeys.size()]));
			jedis.del(keys.toArray(new String[keys.size()]));
			return "Plan deleted successfully";
		}
		response.setStatus(HttpServletResponse.SC_NOT_FOUND);
		return "Plan does not exist";
	}

	@RequestMapping(value = "/plan/{id}", method = RequestMethod.PUT)
	public @ResponseBody String updatePlan(HttpServletRequest request, HttpServletResponse response,
			@PathVariable String id, @RequestBody String object)
			throws JSONException, ProcessingException, IOException {
		if (request.getHeader("Authorization") == null) {
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
			return "Please provide authorization token";
		}

		if (!tokenValidation(request)) {
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
			return "Unauthorised";
		}
		Map<String, String> plan = jedis.hgetAll("plan_" + id);
		if (!plan.isEmpty()) {
			String headerETag = request.getHeader("If-Match");
			String storedEtag = plan.get("etag");
			if (headerETag == null || storedEtag == null || headerETag.equals(storedEtag)) {
				String saveStatus = savePlan(request, response, object);
				if (saveStatus.equals("Invalid JSON")) {
					return saveStatus;
				} else {
					return "Plan updated successfully";
				}
			} else {
				response.setStatus(HttpServletResponse.SC_PRECONDITION_FAILED);
				return "Plan modified!! Please get the latest etag.";
			}
		} else {
			response.setStatus(HttpServletResponse.SC_NOT_FOUND);
			return "Plan does not exist";
		}

	}

	public static Map<String, String> toMap(JSONObject object) throws JSONException {
		Map<String, String> map = new HashMap<String, String>();
		Iterator<String> keysItr = object.keys();
		while (keysItr.hasNext()) {
			String key = keysItr.next();
			Object value = object.get(key);
			if (value instanceof JSONObject) {
				value = toMap((JSONObject) value);
			}
			map.put(key, String.valueOf(value));
		}
		return map;
	}

	public String generateETagHeaderValue(InputStream inputStream) throws IOException {
		StringBuilder builder = new StringBuilder(37);
		DigestUtils.appendMd5DigestAsHex(inputStream, builder);
		return builder.toString();
	}

	public boolean tokenValidation(HttpServletRequest request) {
		final String secretKey = "deception";
		String header = request.getHeader("Authorization");
		if (header.contains("Bearer")) {
			String token = header.split(" ")[1];
			String decryptedToken = AES.decrypt(token, secretKey);
			JSONObject js;
			try {
				js = new JSONObject(decryptedToken);
				Date cur_time = new Date();
				Date token_Time = new Date(js.getString("expiry"));
				if (token_Time.compareTo(cur_time) > 0) {
					return true;
				}
			} catch (Exception e) {
				return false;
			}
		}
		return false;
	}

	@RequestMapping(value = "/planschema", method = RequestMethod.POST)
	public @ResponseBody String savePlanSchema(HttpServletRequest request, HttpServletResponse response,@RequestBody String object) throws JSONException, ProcessingException, IOException {
		JSONObject errorObject = new JSONObject();
		if (request.getHeader("Authorization") == null) {
			errorObject.put("Error", "Not Authorized");
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
			return errorObject.toString();
		}
		if (!tokenValidation(request)) {
			errorObject.put("Error", "Not Authorized");
			response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
			return errorObject.toString();
		}
		jedis.set("schema", object);
		return "";
	}

}