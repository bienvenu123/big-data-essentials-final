from rest_framework import serializers


class TelecomEventSerializer(serializers.Serializer):
    """Validate the shared event envelope before it is published to Kafka."""

    event_id = serializers.UUIDField()
    event_time = serializers.DateTimeField()
    event_type = serializers.ChoiceField(
        choices=["usage", "invoice", "payment", "support_call", "plan_change"]
    )
    customer_id = serializers.IntegerField(min_value=1)
    customer = serializers.DictField()
    payload = serializers.DictField()

    def validate_customer(self, value):
        required = {"contract_type", "internet_service", "payment_method", "tenure_months"}
        missing = required - set(value)
        if missing:
            raise serializers.ValidationError(f"Missing customer fields: {sorted(missing)}")
        return value
