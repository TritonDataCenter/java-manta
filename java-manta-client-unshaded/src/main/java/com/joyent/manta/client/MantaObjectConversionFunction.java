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
        String name = Validate.notNull(item.get("name"), "Filename is null").toString();
        String mtime = Validate.notNull(item.get("mtime"), "Modification time is null").toString();
        String type = Validate.notNull(item.get("type"), "File type is null").toString();

        String objPath = String.format("%s%s%s",
                StringUtils.removeEnd(item.get("path").toString(), SEPARATOR),
                SEPARATOR,
                StringUtils.removeStart(name, SEPARATOR));
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setLastModified(mtime);

        /* We look for contentType explicitly because it is being added to Manta
         * in a future version and this property may not be available on all
         * Manta installs for quite some time. */
        if (item.containsKey("contentType")) {
            String contentType = Objects.toString(item.get("contentType"), null);
            headers.setContentType(contentType);
        } else if (type.equals(MantaObject.MANTA_OBJECT_TYPE_DIRECTORY)) {
            headers.setContentType(MantaObjectResponse.DIRECTORY_RESPONSE_CONTENT_TYPE);
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

        // This property may not be available on all Manta installs for quite some time
        if (item.containsKey("contentMD5")) {
            String contentMD5 = Objects.toString(item.get("contentMD5"), null);
            headers.setContentMD5(contentMD5);
        }

        return new MantaObjectResponse(formatPath(objPath), headers);
    }
}
