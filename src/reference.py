"""生成期间共享的维度索引和商品价格生命周期"""

from __future__ import annotations

import hashlib
from bisect import bisect_right
from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any

from .plan_store import PlanStore

from .settings import RunContext
from .support import (
    UNKNOWN_ID,
    build_version_index,
    iter_jsonl_rows,
    price,
)


@dataclass(frozen=True, slots=True)
class PricePoint:
    effective_date: date
    list_price: Decimal
    sale_price: Decimal
    cost_price: Decimal
    reason_code: str
    reason_description: str


@dataclass(frozen=True, slots=True)
class ProductProfile:
    sku: dict[str, Any]
    spu: dict[str, Any]
    shop: dict[str, Any]
    category: dict[str, Any]
    brand: dict[str, Any] | None
    listing_date: date
    warning_stock_qty: int
    initial_stock_qty: int
    price_points: tuple[PricePoint, ...]

    def price_on(self, day: date) -> PricePoint:
        selected = self.price_points[0]
        for point in self.price_points:
            if point.effective_date > day:
                break
            selected = point
        return selected


@dataclass(slots=True)
class ReferenceData:
    user_versions: dict[int, list[dict[str, Any]]]
    current_users: list[dict[str, Any]]
    user_registration_times: list[datetime]
    tags_by_code: dict[str, dict[str, Any]]
    user_tag_relations: dict[int, list[dict[str, Any]]]
    sellers_by_id: dict[int, dict[str, Any]]
    channels: list[dict[str, Any]]
    pages_by_id: dict[str, dict[str, Any]]
    payments: list[dict[str, Any]]
    logistics: list[dict[str, Any]]
    warehouses: list[dict[str, Any]]
    warehouse_by_sku: dict[int, dict[str, Any]]
    regions_by_district: dict[str, dict[str, Any]]
    promotions: list[dict[str, Any]]
    coupons: list[dict[str, Any]]
    promotion_scopes: dict[int, list[dict[str, Any]]]
    coupon_scopes: dict[int, list[dict[str, Any]]]
    profiles: list[ProductProfile]
    profile_by_sku: dict[int, ProductProfile]
    profiles_by_listing_date: dict[date, list[ProductProfile]]
    price_points_by_date: dict[date, list[tuple[ProductProfile, PricePoint]]]

    def active_profiles(self, day: date) -> list[ProductProfile]:
        return [profile for profile in self.profiles if profile.listing_date <= day]

    def active_users(self, moment: datetime) -> list[dict[str, Any]]:
        end = bisect_right(self.user_registration_times, moment)
        return self.current_users[:end]

    def warehouse_for_profile(
        self,
        profile: ProductProfile,
    ) -> dict[str, Any]:
        return self.warehouse_by_sku[int(profile.sku["sku_id"])]

    def service_warehouse(
        self,
        region: dict[str, Any] | None,
    ) -> dict[str, Any]:
        province_code = str(region.get("province_code") or "") if region else ""
        direct = [
            warehouse
            for warehouse in self.warehouses
            if province_code
            and str(warehouse.get("province_code") or "") == province_code
        ]
        if direct:
            return direct[0]
        zone = _province_service_zone(province_code)
        regional = [
            warehouse
            for warehouse in self.warehouses
            if _province_service_zone(str(warehouse.get("province_code") or "")) == zone
        ]
        candidates = regional or self.warehouses
        index = _stable_int(f"service-warehouse:{province_code or 'unknown'}") % len(
            candidates
        )
        return candidates[index]

    def active_promotions(self, moment: datetime) -> list[dict[str, Any]]:
        return [
            row
            for row in self.promotions
            if row["activity_start_time"] <= moment < row["activity_end_time"]
        ]

    def active_coupons(self, moment: datetime) -> list[dict[str, Any]]:
        return [
            row
            for row in self.coupons
            if row["use_start_time"] <= moment < row["use_end_time"]
        ]

    def promotion_applies(
        self,
        promotion_id: int,
        profile: ProductProfile,
    ) -> bool:
        return _scope_applies(
            self.promotion_scopes.get(promotion_id, []),
            profile,
        )

    def coupon_applies(
        self,
        coupon_template_id: int,
        profile: ProductProfile,
    ) -> bool:
        return _scope_applies(
            self.coupon_scopes.get(coupon_template_id, []),
            profile,
        )


def warning_stock_qty_for_sku(sku_id: int) -> int:
    return 2 + _stable_int(f"warning:{sku_id}") % 8


def _province_service_zone(province_code: str) -> str:
    prefix = province_code[:2]
    if prefix in {"11", "12", "13", "14", "15"}:
        return "NORTH"
    if prefix in {"21", "22", "23"}:
        return "NORTHEAST"
    if prefix in {"31", "32", "33", "34", "35", "36", "37"}:
        return "EAST"
    if prefix in {"41", "42", "43", "44", "45", "46"}:
        return "CENTRAL_SOUTH"
    if prefix in {"50", "51", "52", "53", "54"}:
        return "SOUTHWEST"
    if prefix in {"61", "62", "63", "64", "65"}:
        return "NORTHWEST"
    return "UNKNOWN"


def load_reference_data(ctx: RunContext, store: PlanStore) -> ReferenceData:
    def active(table, field=None):
        return [
            row
            for row in store.rows(table)
            if row.get("is_current", 1) == 1
            and (field is None or row[field] not in (UNKNOWN_ID, "UNKNOWN"))
        ]

    users = [
        row for row in store.rows("dim_user_info_zip") if row["user_id"] != UNKNOWN_ID
    ]
    tags = active("dim_user_tag_info", "tag_code")
    tag_relations = active("bridge_user_tag_relation_zip")
    shops = active("dim_shop_info_zip", "shop_id")
    sellers = active("dim_seller_info_zip", "seller_id")
    skus = active("dim_sku_info_zip", "sku_id")
    spus = active("dim_spu_info_zip", "spu_id")
    categories = active("dim_category_info_zip")
    brands = active("dim_brand_info")
    channels = active("dim_channel_info", "channel_code")
    pages = active("dim_page_info")
    payments = active("dim_payment_type", "payment_type_code")
    logistics = active("dim_logistics_company", "logistics_company_id")
    warehouses = active("dim_warehouse_info_zip", "warehouse_id")
    regions = active("dim_geo_region_zip", "region_code")
    promotions = active("dim_promotion_rule_version", "promotion_id")
    coupons = active("dim_coupon_template_version", "coupon_template_id")
    promotion_scope_rows = active("bridge_promotion_scope")
    coupon_scope_rows = active("bridge_coupon_scope")

    spu_by_id = {int(row["spu_id"]): row for row in spus}
    shop_by_id = {int(row["shop_id"]): row for row in shops}
    category_by_id = {int(row["category_id"]): row for row in categories}
    brand_by_id = {int(row["brand_id"]): row for row in brands}
    source_prices = _source_prices(ctx)
    profiles = [
        _build_profile(
            ctx,
            sku,
            spu_by_id[int(sku["spu_id"])],
            shop_by_id[int(sku["shop_id"])],
            category_by_id[int(sku["category_id"])],
            brand_by_id.get(int(sku["brand_id"])),
            source_prices[int(sku["sku_id"])],
        )
        for sku in skus
    ]
    profiles.sort(
        key=lambda profile: (
            _stable_int(f"sku-demand:{profile.sku['sku_id']}") % 1_000_003
            + (_stable_int(f"brand-demand:{profile.sku['brand_id']}") % 1_000_003)
            * 0.18
            + (_stable_int(f"shop-demand:{profile.sku['shop_id']}") % 1_000_003) * 0.12,
            int(profile.sku["sku_id"]),
        )
    )
    by_listing_date: dict[date, list[ProductProfile]] = defaultdict(list)
    price_points_by_date: dict[date, list[tuple[ProductProfile, PricePoint]]] = (
        defaultdict(list)
    )
    for profile in profiles:
        by_listing_date[profile.listing_date].append(profile)
        for point in profile.price_points:
            price_points_by_date[point.effective_date].append((profile, point))
    promotion_scopes: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in promotion_scope_rows:
        promotion_scopes[int(row["promotion_id"])].append(row)
    coupon_scopes: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in coupon_scope_rows:
        coupon_scopes[int(row["coupon_template_id"])].append(row)
    current_users = sorted(
        (row for row in users if row["is_current"] == 1),
        key=lambda row: row["register_time"] or datetime(1900, 1, 1),
    )
    tag_by_sk = {int(row["user_tag_sk"]): row for row in tags}
    user_tag_relations: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for relation in tag_relations:
        tag = tag_by_sk.get(int(relation["user_tag_sk"]))
        if tag is None:
            continue
        user_tag_relations[int(relation["user_id"])].append(
            relation | {"tag_code": tag["tag_code"]}
        )
    warehouses.sort(key=lambda row: int(row["warehouse_id"]))
    return ReferenceData(
        user_versions=build_version_index(users, "user_id"),
        current_users=current_users,
        user_registration_times=[
            row["register_time"] or datetime(1900, 1, 1) for row in current_users
        ],
        tags_by_code={str(row["tag_code"]): row for row in tags},
        user_tag_relations=dict(user_tag_relations),
        sellers_by_id={int(row["seller_id"]): row for row in sellers},
        channels=channels,
        pages_by_id={str(row["page_id"]): row for row in pages},
        payments=payments,
        logistics=logistics,
        warehouses=warehouses,
        warehouse_by_sku={
            int(profile.sku["sku_id"]): warehouses[
                _stable_int(f"sku-warehouse:{profile.sku['sku_id']}") % len(warehouses)
            ]
            for profile in profiles
        },
        regions_by_district={
            str(row["district_code"]): row
            for row in regions
            if row.get("district_code") is not None
        },
        promotions=promotions,
        coupons=coupons,
        promotion_scopes=dict(promotion_scopes),
        coupon_scopes=dict(coupon_scopes),
        profiles=profiles,
        profile_by_sku={int(profile.sku["sku_id"]): profile for profile in profiles},
        profiles_by_listing_date=dict(by_listing_date),
        price_points_by_date=dict(price_points_by_date),
    )


def _source_prices(ctx: RunContext) -> dict[int, dict[str, Any]]:
    result: dict[int, dict[str, Any]] = {}
    for product in iter_jsonl_rows(ctx.gen.data_dir / "lineage.jsonl"):
        for sku in product["skus"]:
            result[int(sku["sku_id"])] = sku
    return result


def _build_profile(
    ctx: RunContext,
    sku: dict[str, Any],
    spu: dict[str, Any],
    shop: dict[str, Any],
    category: dict[str, Any],
    brand: dict[str, Any] | None,
    source_price: dict[str, Any],
) -> ProductProfile:
    sku_id = int(sku["sku_id"])
    listing_time = spu.get("on_shelf_time") or spu["effective_start_time"]
    listing_date = listing_time.date()
    current_sale = price(source_price["origin_sale_price_cny"])
    current_list = price(source_price.get("origin_list_price_cny") or current_sale)
    margin_basis = 58 + _stable_int(f"margin:{sku_id}") % 23
    cost_ratio = Decimal(margin_basis) / Decimal("100")
    initial_factor = Decimal(
        92 + _stable_int(f"initial-price:{sku_id}") % 21
    ) / Decimal("100")
    # 价格轨迹只依赖 SKU 和上市日期，延长生成范围不会改写历史价格。
    dates = [listing_date]
    cursor = listing_date
    while True:
        interval = 75 + _stable_int(f"price-step:{sku_id}:{cursor}") % 91
        cursor += timedelta(days=interval)
        if cursor > ctx.gen.end_date:
            break
        dates.append(cursor)
    points: list[PricePoint] = []
    for index, event_date in enumerate(dates):
        wave = Decimal(_stable_int(f"price-wave:{sku_id}:{index}") % 9 - 4) / Decimal(
            "100"
        )
        factor = initial_factor if index == 0 else Decimal("1") + wave
        sale = price(current_sale * factor)
        points.append(
            PricePoint(
                effective_date=event_date,
                list_price=price(max(sale, current_list * factor)),
                sale_price=sale,
                cost_price=price(sale * cost_ratio),
                reason_code="INITIAL" if index == 0 else "MARKET_ADJUSTMENT",
                reason_description="商品上架定价" if index == 0 else "市场价格周期调整",
            )
        )
    return ProductProfile(
        sku=sku,
        spu=spu,
        shop=shop,
        category=category,
        brand=brand,
        listing_date=listing_date,
        warning_stock_qty=warning_stock_qty_for_sku(sku_id),
        initial_stock_qty=(
            max(
                1,
                warning_stock_qty_for_sku(sku_id)
                - 2
                + _stable_int(f"initial-stock:{sku_id}") % 6,
            )
        ),
        price_points=tuple(points),
    )


def _stable_int(value: str) -> int:
    return int(hashlib.sha256(value.encode()).hexdigest()[:16], 16)


def _scope_applies(
    scopes: list[dict[str, Any]],
    profile: ProductProfile,
) -> bool:
    included = False
    for scope in scopes:
        scope_type = str(scope["scope_type"])
        business_id = str(scope["scope_business_id"])
        matches = (
            scope_type == "ALL"
            or (scope_type == "SHOP" and business_id == str(profile.shop["shop_id"]))
            or (
                scope_type == "CATEGORY"
                and business_id == str(profile.category["category_id"])
            )
        )
        if matches and int(scope["is_excluded"]):
            return False
        included = included or matches
    return included
