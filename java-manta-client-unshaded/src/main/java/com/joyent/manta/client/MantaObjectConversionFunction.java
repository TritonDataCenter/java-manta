/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client;

import com.joyent.manta.http.MantaHttpHeaders;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.http.entity.ContentType;

import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import static com.joyent.manta.client.MantaClient.SEPARATOR;
import static com.joyent.manta.util.MantaUtils.formatPath;

/**
 * Function class that provides the conversion method for mapping a {@link Map}
 * to a {@link MantaObject}.
 *
 * @author <a href="https://github.com/dekobon">Elijah Zupancic</a>
 * @since 3.1.7
 */
public class MantaObjectConversionFunction implements Function<Map<String, Object>, MantaObject> {
    /**
     * Static instance to be used as a singleton.
     */
    static final MantaObjectConversionFunction INSTANCE = new MantaObjectConversionFunction();

    @Override
    public MantaObject apply(final Map<String, Object> item) {
        String name = Objects.toString(item.get("name"));
        String mtime = Objects.toString(item.get("mtime"));
        String type = Objects.toString(item.get("type"));
        Validate.notNull(name, "File name must not be null");
        String objPath = String.format("%s%s%s",
                StringUtils.removeEnd(item.get("path").toString(), SEPARATOR),
                SEPARATOR,
                StringUtils.removeStart(name, SEPARATOR));
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setLastModified(mtime);

        if (type.equals(MantaObject.MANTA_OBJECT_TYPE_DIRECTORY)) {
            headers.setContentType(MantaObjectResponse.DIRECTORY_RESPONSE_CONTENT_TYPE);
        } else {
            headers.setContentType(ContentType.APPLICATION_OCTET_STREAM.toString());
        }

        if (item.containsKey("etag")) {
            headers.setETag(Objects.toString(item.get("etag")));
        }

        if (item.containsKey("size")) {
            long size = Long.parseLong(Objects.toString(item.get("size")));
            headers.setContentLength(size);
        }

        if (item.containsKey("durability")) {
            String durabilityString = Objects.toString(item.get("durability"));
            if (durabilityString != null) {
                int durability = Integer.parseInt(durabilityString);
                headers.setDurabilityLevel(durability);
            }
        }

        return new MantaObjectResponse(formatPath(objPath), headers);
    }
}
