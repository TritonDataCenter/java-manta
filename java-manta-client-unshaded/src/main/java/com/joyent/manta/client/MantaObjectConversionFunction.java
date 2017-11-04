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
     * Key for name property in directory listing results.
     */
    private static final String NAME_FIELD_KEY = "name";

    /**
     * Key for modification time property in directory listing results.
     */
    private static final String MTIME_FIELD_KEY = "mtime";

    /**
     * Key for object type (dir / file) property in directory listing results.
     */
    private static final String TYPE_FIELD_KEY = "type";

    /**
     * Key for injecting the path to the object in the results returned from apply().
     */
    private static final String PATH_FIELD_KEY = "path";

    /**
     * Key for content-type property in directory listing results.
     */
    private static final String CONTENT_TYPE_FIELD_KEY = "contentType";

    /**
     * Key for e-tag property in directory listing results.
     */
    private static final String ETAG_FIELD_KEY = "etag";

    /**
     * Key for object size property in directory listing results.
     */
    private static final String SIZE_FIELD_KEY = "size";

    /**
     * Key for durability (number of copies) property in directory listing results.
     */
    private static final String DURABILITY_FIELD_KEY = "durability";

    /**
     * Key for content-md5 property in directory listing results.
     */
    private static final String CONTENT_MD5_FIELD_KEY = "contentMD5";

    /**
     * Static instance to be used as a singleton.
     */
    public static final MantaObjectConversionFunction INSTANCE = new MantaObjectConversionFunction();

    @Override
    public MantaObject apply(final Map<String, Object> item) {
        String name = Validate.notNull(item.get(NAME_FIELD_KEY), "Filename is null").toString();
        String mtime = Validate.notNull(item.get(MTIME_FIELD_KEY), "Modification time is null").toString();
        String type = Validate.notNull(item.get(TYPE_FIELD_KEY), "File type is null").toString();

        String objPath = String.format("%s%s%s",
                StringUtils.removeEnd(item.get(PATH_FIELD_KEY).toString(), SEPARATOR),
                SEPARATOR,
                StringUtils.removeStart(name, SEPARATOR));
        MantaHttpHeaders headers = new MantaHttpHeaders();
        headers.setLastModified(mtime);

        /* We look for contentType explicitly because it is being added to Manta
         * in a future version and this property may not be available on all
         * Manta installs for quite some time. */
        if (item.containsKey(CONTENT_TYPE_FIELD_KEY)) {
            String contentType = Objects.toString(item.get(CONTENT_TYPE_FIELD_KEY), null);
            headers.setContentType(contentType);
        } else if (type.equals(MantaObject.MANTA_OBJECT_TYPE_DIRECTORY)) {
            headers.setContentType(MantaObjectResponse.DIRECTORY_RESPONSE_CONTENT_TYPE);
        }

        if (item.containsKey(ETAG_FIELD_KEY)) {
            headers.setETag(Objects.toString(item.get(ETAG_FIELD_KEY)));
        }

        if (item.containsKey(SIZE_FIELD_KEY)) {
            long size = Long.parseLong(Objects.toString(item.get(SIZE_FIELD_KEY)));
            headers.setContentLength(size);
        }

        if (item.containsKey(DURABILITY_FIELD_KEY)) {
            String durabilityString = Objects.toString(item.get(DURABILITY_FIELD_KEY));
            if (durabilityString != null) {
                int durability = Integer.parseInt(durabilityString);
                headers.setDurabilityLevel(durability);
            }
        }

        // This property may not be available on all Manta installs for quite some time
        if (item.containsKey(CONTENT_MD5_FIELD_KEY)) {
            String contentMD5 = Objects.toString(item.get(CONTENT_MD5_FIELD_KEY), null);
            headers.setContentMD5(contentMD5);
        }

        return new MantaObjectResponse(formatPath(objPath), headers);
    }
}
