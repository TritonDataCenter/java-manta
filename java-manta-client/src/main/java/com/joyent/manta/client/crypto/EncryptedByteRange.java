/*
 * Copyright (c) 2017, Joyent, Inc. All rights reserved.
 *
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 */
package com.joyent.manta.client.crypto;

import com.joyent.manta.client.AdjustableRange;
import com.joyent.manta.client.ClosedRange;
import com.joyent.manta.client.DecoratedRange;
import com.joyent.manta.client.FixableRange;
import com.joyent.manta.client.NullRange;
import com.joyent.manta.client.OpenRange;
import com.joyent.manta.client.Range;

/**
 * Interface describing an encryption byte range decorator.
 *
 * @param <T> type of a decorated {@code NullRange}
 * @param <U> type of a decorated {@code OpenRange}
 * @param <V> type of a decorated {@code ClosedRange}
 * @param <W> type of decorated {@code Range}
 *
 * @author <a href="https://github.com/uxcn">Jason Schulz</a>
 * @since 3.0.0
 */
public interface EncryptedByteRange<T extends NullRange & FixableRange<T> & AdjustableRange<T, U, V>,
                                    U extends OpenRange & FixableRange<U> & AdjustableRange<T, U, V>,
                                    V extends ClosedRange & FixableRange<V> & AdjustableRange<T, U, V>,
                                    W extends Range>
       extends EncryptedRange,
               AdjustableRange<EncryptedNullByteRange<T, U, V>,
                               EncryptedOpenByteRange<T, U, V>,
                               EncryptedClosedByteRange<T, U, V>>,
               DecoratedRange<W> { }
