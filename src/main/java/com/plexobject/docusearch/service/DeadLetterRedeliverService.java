package com.plexobject.docusearch.service;

import javax.ws.rs.core.Response;

public interface DeadLetterRedeliverService {

    Response redeliver(String brokerName, String to);
}
