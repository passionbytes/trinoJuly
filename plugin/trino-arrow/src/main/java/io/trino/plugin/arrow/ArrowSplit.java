/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.arrow;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.trino.spi.HostAddress;
import io.trino.spi.connector.ConnectorSplit;

import java.net.URI;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class ArrowSplit
        implements ConnectorSplit
{
    private final List<URI> uri;
    private final byte[] ticket;

    @JsonCreator
    public ArrowSplit(List<URI> uri, byte[] ticket)
    {
        this.uri = ImmutableList.copyOf(requireNonNull(uri, "uri is null"));
        this.ticket = requireNonNull(ticket, "ticket is null");
    }

    @JsonProperty
    public List<URI> getUri()
    {
        return uri;
    }

    @JsonProperty
    public byte[] getTicket()
    {
        return ticket;
    }

    @Override
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

}
