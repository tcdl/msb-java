package io.github.tcdl.msb.api;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A message template to be used to construct a request/response message.
 */
public class MessageTemplate {

    private Integer ttl;
    private List<String> tags;

    public MessageTemplate() {
        tags = new ArrayList<>();
    }

    /**
     * Copy constructor for message template
     * Creates a new instance of MessageTemplate and initializes it from given instance
     * @param copyFrom
     */
    private MessageTemplate(MessageTemplate copyFrom) {
        this();
        if (copyFrom != null) {
            this.ttl = copyFrom.getTtl();
            tags.addAll(copyFrom.getTags());
        }
    }

    public static MessageTemplate copyOf(MessageTemplate from) {
        return new MessageTemplate(from);
    }

    /**
     *
     * @return time to live for the message
     */
    public Integer getTtl() {
        return ttl;
    }

    /**
     *
     * @param ttl time to live for the message
     */
    public MessageTemplate withTtl(Integer ttl) {
        this.ttl = ttl;
        return this;
    }

    /**
     * @return tags of the message
     */
    public List<String> getTags() {
        return tags;
    }

    /**
     * @param tags of the message
     */
    public MessageTemplate withTags(String... tags) {
        this.tags.addAll(Arrays.asList(tags));
        return  this;
    }

    public void addTags(String... tags) {
        this.tags.addAll(Arrays.asList(tags));
    }

    /**
     * @deprecated because of misleading signature and complete duplication of {@link #withTags(String...)} logic.
     * Method will be removed in version 1.7
     * todo remove it
     */
    @Deprecated
    public MessageTemplate addTag(String... tags) {
        this.tags.addAll(Arrays.asList(tags));
        return  this;
    }

    public void addTag(String tag) {
        this.tags.add(tag);
    }

    @Override
    public String toString() {
        return "MessageTemplate [ttl=" + ttl + ", tags=" + StringUtils.join(tags, ",") + "]";
    }
}
