package com.urbanairship.datacube.metrics;

import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;

public final class MetricNameDetails {

    private final String group;
    private final String type;
    private final Optional<String> scope;
    private final String name;

    public static Builder newBuilder() {
        return new Builder();
    }

    private MetricNameDetails(String group, String type, Optional<String> scope, String name) {
        this.group = group;
        this.type = type;
        this.scope = scope;
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        MetricNameDetails that = (MetricNameDetails) o;

        if (!group.equals(that.group)) {
            return false;
        }
        if (!name.equals(that.name)) {
            return false;
        }
        if (!scope.equals(that.scope)) {
            return false;
        }
        if (!type.equals(that.type)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = group.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + scope.hashCode();
        result = 31 * result + name.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("group", group)
                .add("type", type)
                .add("scope", scope)
                .add("name", name)
                .toString();
    }

    public String getFormattedJmxName() {
        StringBuilder mbeanNameBuilder = new StringBuilder()
                .append(group)
                .append(":type=").append(type);

        if (scope.isPresent()) {
            mbeanNameBuilder.append(",scope=").append(scope.get());
        }

        return mbeanNameBuilder.append(",name=").append(name).toString();
    }

    public static final class Builder {

        private String group = null;
        private String type = null;
        private String scope = null;
        private String name = null;

        private Builder() { }

        public Builder setGroup(String group) {
            this.group = group;
            return this;
        }

        public Builder setType(String type) {
            this.type = type;
            return this;
        }

        public Builder setScope(String scope) {
            this.scope = scope;
            return this;
        }

        public Builder setName(String name) {
            this.name = name;
            return this;
        }

        public MetricNameDetails build() {
            Preconditions.checkNotNull(group);
            Preconditions.checkNotNull(type);
            Preconditions.checkNotNull(name);

            return new MetricNameDetails(
                    group,
                    type,
                    StringUtils.isNotBlank(scope) ? Optional.of(scope) : Optional.<String>absent(),
                    name
            );
        }
    }
}
