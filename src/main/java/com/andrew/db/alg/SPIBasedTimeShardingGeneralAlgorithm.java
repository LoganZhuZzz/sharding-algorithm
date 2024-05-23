package com.andrew.db.alg;

import com.google.common.base.Preconditions;
import com.google.common.collect.BoundType;
import com.google.common.collect.Range;
import groovy.util.logging.Slf4j;
import lombok.Data;
import org.apache.shardingsphere.infra.config.exception.ShardingSphereConfigurationException;
import org.apache.shardingsphere.sharding.api.sharding.standard.PreciseShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.RangeShardingValue;
import org.apache.shardingsphere.sharding.api.sharding.standard.StandardShardingAlgorithm;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAdjusters;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.time.Month.*;

@Slf4j
@Data
public class SPIBasedTimeShardingGeneralAlgorithm implements StandardShardingAlgorithm<Integer> {
    private Properties props = new Properties();

    private static final String DATE_TIME_PATTERN_KEY = "datetime-pattern";

    private static final String DATE_TIME_LOWER_KEY = "datetime-lower";

    // 非必要配置 如果没有配置则使用当前系统时间
    private static final String DATE_TIME_UPPER_KEY = "datetime-upper";

    private DateTimeFormatter dateTimeFormatter;

    private int dateTimePatternLength;

    private LocalDateTime dateTimeLower;

    private LocalDateTime dateTimeUpper;

    @Override
    public void init() {
        String dateTimePattern = getDateTimePattern();
        dateTimeFormatter = DateTimeFormatter.ofPattern(dateTimePattern);
        dateTimePatternLength = dateTimePattern.length();
        dateTimeLower = getDateTimeLower(dateTimePattern);
        dateTimeUpper = getDateTimeUpper(dateTimePattern);
    }

    @Override
    public String doSharding(Collection<String> collection, PreciseShardingValue<Integer> preciseShardingValue) {
        Integer value = preciseShardingValue.getValue();
        LocalDateTime localDateTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(value), ZoneId.systemDefault());
        return doSharding(collection, Range.singleton(localDateTime)).stream().findFirst().orElse(null);
    }

    @Override
    public Collection<String> doSharding(Collection<String> collection, RangeShardingValue<Integer> rangeShardingValue) {
        Range<Integer> valueRange = rangeShardingValue.getValueRange();
        Integer low = valueRange.lowerEndpoint();
        Integer up = valueRange.upperEndpoint();
        LocalDateTime lowTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(low), ZoneId.systemDefault());
        LocalDateTime upTime = LocalDateTime.ofInstant(Instant.ofEpochSecond(up), ZoneId.systemDefault());
        Range<LocalDateTime> actualRange = Range.open(lowTime, upTime);
        return doSharding(collection, actualRange);
    }

    private Collection<String> doSharding(final Collection<String> availableTargetNames, final Range<LocalDateTime> range) {
        Set<String> result = new HashSet<>();
        LocalDateTime calculateTime = dateTimeLower;
        while (!calculateTime.isAfter(dateTimeUpper)) {
            /*
             * 汛期处理: 按周分表
             * 每次增加一周判断是否存在交集
             * 注意 如果下次时间存在跨月情况 改为下月第一天开始时间 保证非汛期按月分表
             *
             * 非汛期处理: 按月分表
             */
            LocalDateTime next;
            if (isFloodSeasonMonth(calculateTime)) {
                next = calculateTime.plus(1, ChronoUnit.WEEKS);
                // 跨月需要处理
                if (isNextMonth(next, calculateTime)) {
                    next = getStartTimeOfMonth(next);
                }
            } else {
                next = calculateTime.plus(1, ChronoUnit.MONTHS);
            }

            // 如果小于最大时间 判断是否存在交集
            if (hasIntersection(Range.closedOpen(calculateTime, next), range)) {
                // 存在交集 添加所有匹配实际表
                result.addAll(getMatchedTables(calculateTime, availableTargetNames));
            }
            calculateTime = next;
        }

        return result;
    }

    private LocalDateTime getStartTimeOfMonth(LocalDateTime next) {
        return LocalDateTime.of(next.with(TemporalAdjusters.firstDayOfMonth()).toLocalDate(),
                LocalTime.MIN);
    }

    private boolean isNextMonth(LocalDateTime next, LocalDateTime calculateTime) {
        return next.getMonthValue() - calculateTime.getMonthValue() == 1;
    }

    /**
     * 判断传入时间是否为汛期
     *
     * @param time 传入时间
     * @return 判断结果
     */
    private boolean isFloodSeasonMonth(LocalDateTime time) {
        return time.getMonth().equals(JUNE) || time.getMonth().equals(JULY)
                || time.getMonth().equals(AUGUST) || time.getMonth().equals(SEPTEMBER);
    }

    /**
     * 从actual-data-nodes中获取匹配入参时间的的列表
     *
     * @param dateTime             入参时间
     * @param availableTargetNames actual-data-nodes中配置的所以节点
     * @return 匹配的集合
     */
    private Collection<String> getMatchedTables(LocalDateTime dateTime, Collection<String> availableTargetNames) {
        String tableSuffix = getTableSuffix(dateTime);
        return availableTargetNames.parallelStream().filter(each -> each.endsWith(tableSuffix)).collect(Collectors.toSet());
    }

    private String getTableSuffix(LocalDateTime dateTime) {
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy_MM").toFormatter();
        String tableSuffix = formatter.format(dateTime);

        if (isFloodSeasonMonth(dateTime)) {
            int week;
            int dayOfMonth = dateTime.getDayOfMonth();
            if (dayOfMonth >= 21) {
                week = 4;
            } else {
                week = (int) Math.ceil(dateTime.getDayOfMonth() / 7.0d);
            }
            tableSuffix = String.format("%s_%d", tableSuffix, week);
        }
        return tableSuffix;
    }

    private boolean hasIntersection(final Range<LocalDateTime> calculateRange, final Range<LocalDateTime> range) {
        LocalDateTime lower = range.hasLowerBound() ? range.lowerEndpoint() : dateTimeLower;
        LocalDateTime upper = range.hasUpperBound() ? range.upperEndpoint() : dateTimeUpper;
        BoundType lowerBoundType = range.hasLowerBound() ? range.lowerBoundType() : BoundType.CLOSED;
        BoundType upperBoundType = range.hasUpperBound() ? range.upperBoundType() : BoundType.CLOSED;
        Range<LocalDateTime> dateTimeRange = Range.range(lower, lowerBoundType, upper, upperBoundType);
        return calculateRange.isConnected(dateTimeRange) && !calculateRange.intersection(dateTimeRange).isEmpty();
    }

    /**
     * 获取yml配置的key
     *
     * @return value
     * @throws IllegalArgumentException 如果未进行配置
     */
    private String getDateTimePattern() {
        Preconditions.checkArgument(props.containsKey(DATE_TIME_PATTERN_KEY), "%s can not be null.",
                DATE_TIME_PATTERN_KEY);
        return props.getProperty(DATE_TIME_PATTERN_KEY);
    }

    private LocalDateTime getDateTimeLower(final String dateTimePattern) {
        Preconditions.checkArgument(props.containsKey(DATE_TIME_LOWER_KEY), "%s can not be null.",
                DATE_TIME_LOWER_KEY);
        return getDateTime(DATE_TIME_LOWER_KEY, props.getProperty(DATE_TIME_LOWER_KEY), dateTimePattern);
    }

    /**
     * 获取最大时间 如果未进行配置则默认为当前时间
     *
     * @param dateTimePattern 时间格式化字符串(必要配置)
     * @return 最大时间
     */
    private LocalDateTime getDateTimeUpper(final String dateTimePattern) {
        return props.containsKey(DATE_TIME_UPPER_KEY)
                ? getDateTime(DATE_TIME_UPPER_KEY, props.getProperty(DATE_TIME_UPPER_KEY), dateTimePattern)
                : LocalDateTime.now();
    }

    private LocalDateTime getDateTime(final String dateTimeKey, final String dateTimeValue,
                                      final String dateTimePattern) {
        try {
            return LocalDateTime.parse(dateTimeValue, dateTimeFormatter);
        } catch (final DateTimeParseException ex) {
            throw new ShardingSphereConfigurationException("Invalid %s, datetime pattern should be `%s`, value is `%s`",
                    dateTimeKey, dateTimePattern, dateTimeValue);
        }
    }

    @Override
    public String getType() {
        return "TIME_GENERAL_SPI_BASED";
    }
}
