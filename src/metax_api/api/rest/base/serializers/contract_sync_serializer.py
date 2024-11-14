import logging

from rest_framework import serializers

from metax_api.models.contract import Contract
from .contract_serializer import ContractSerializer
from .serializer_utils import validate_json

_logger = logging.getLogger(__name__)


# Copied from file_v3_sync_service to avoid circular import issues
class StrictSyncSerializer(serializers.Serializer):
    """Serializer that throws an error for unknown fields."""

    def to_internal_value(self, data):
        if unknown_fields := set(data).difference(self.fields) - {"api_meta"}:
            raise serializers.ValidationError({field: "Unknown field" for field in unknown_fields})
        return super().to_internal_value(data)


class ContractSyncFromV3ListSerializer(serializers.ListSerializer):
    def save(self, **kwargs):
        validated_data = [{**attrs, **kwargs} for attrs in self.validated_data]
        instances = []
        for item in validated_data:
            self.child._errors = None
            self.child._validated_data = item
            instances.append(self.child.save())
        self.instance = instances
        return instances


class ContractSyncFromV3Serializer(StrictSyncSerializer, serializers.ModelSerializer):
    class Meta:
        list_serializer_class = ContractSyncFromV3ListSerializer
        model = Contract
        fields = (
            "id",
            "contract_json",
            # Common fields
            "user_modified",  # Used also for user_created
            "date_modified",
            "date_created",
            "removed",
            "date_removed",
        )

        extra_kwargs = {
            "id": {"read_only": False, "allow_null": True},
            "contract_json": {"required": True},
        }

    def _validate_contract_json(self, value, is_create: bool):
        validate_json(value, self.context["view"].json_schema)
        if is_create:
            self._validate_identifier_uniqueness(value)
        return value

    def _validate_identifier_uniqueness(self, contract_json):
        if Contract.objects.filter(contract_json__identifier=contract_json["identifier"]).exists():
            raise serializers.ValidationError(
                f"identifier {contract_json['identifier']} already exists"
            )

    def save(self):
        validated_data = self._validated_data
        contract_id = validated_data.get("id")
        if contract_id:
            self.instance = Contract.objects_unfiltered.filter(id=contract_id).first()
        else:
            validated_data.pop("id", None)

        identifier = validated_data["contract_json"]["identifier"]
        if not self.instance:
            self.instance = Contract.objects.filter(contract_json__identifier=identifier).first()
        self._validate_contract_json(validated_data["contract_json"], is_create=not self.instance)

        if self.instance:
            self.instance = self.update(self.instance, validated_data)
            _logger.info(f"Sync from V3: Updated contract {self.instance}")
        else:
            self.instance = self.create(validated_data)
            _logger.info(f"Sync from V3: Created contract {self.instance}")

        if validated_data.get("removed"):
            # Common.save unsets removed and date_removed values so we update them separately
            Contract.objects_unfiltered.filter(id=self.instance.id).update(
                removed=True, date_removed=validated_data.get("date_removed")
            )
            self.instance.refresh_from_db()
        return self.instance
