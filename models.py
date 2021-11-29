from jsonfield import JSONField
from django.db import models
from django.contrib.auth import get_user_model
from configs.models import ConditionSet, Languages
from django.contrib.postgres.fields import ArrayField
from .buckets import QPCFileDataStorage
import pytz


class AlembicVersion(models.Model):
    version_num = models.CharField(primary_key=True, max_length=32)

    class Meta:
        db_table = 'alembic_version'


class Tag(models.Model):
    value = models.CharField(max_length=100, unique=True)

    class Meta:
        db_table = 'tag'

    def __str__(self):
        return f'{self.value}'


class Category(models.Model):
    TYPES = (
        ('sleep', 'sleep category'),
        ('energy', 'energy category'),
        ('hydration', 'hydration category'),
        ('stress', 'stress category'),
        ('digestion', 'digestion category'),
        ('muscle', 'muscle category'),
    )
    type = models.CharField(max_length=32, choices=TYPES)
    description = models.CharField(max_length=1024)
    display_name = models.CharField(max_length=32)
    green_best = models.CharField(max_length=256)
    green_worst = models.CharField(max_length=256)
    yellow = models.CharField(max_length=256)
    red = models.CharField(max_length=256)

    def __str__(self):
        return self.display_name

    class Meta:
        db_table = 'category'


class Attribute(models.Model):
    description = models.CharField(max_length=255, blank=True, null=True)
    value = models.CharField(max_length=255)
    group = models.ForeignKey(
        'Group', related_name="attributes", on_delete=models.CASCADE, blank=True, null=True)

    class Meta:
        db_table = 'attribute'

    def __str__(self):
        return f'{self.value}'


class Audios(models.Model):
    user = models.ForeignKey('Users', related_name='audios',
                             on_delete=models.CASCADE, blank=True, null=True)
    researcher_id = models.IntegerField(blank=True, null=True)
    tags = models.ManyToManyField(Tag, blank=True)
    spectrum_url = models.CharField(max_length=300, blank=True, null=True)
    json_url = models.CharField(max_length=300, blank=True, null=True)
    file_name = models.CharField(max_length=300, blank=True, null=True)
    processing_time = models.CharField(max_length=30, blank=True, null=True)
    vitality_algo = models.CharField(max_length=300, blank=True, null=True)
    vitality_score = models.CharField(max_length=100, blank=True, null=True)
    window_size = models.IntegerField(blank=True, null=True)
    passes = models.IntegerField(blank=True, null=True)
    csv_url = models.CharField(max_length=300, blank=True, null=True)
    fundamental_power = models.IntegerField(blank=True, null=True)
    outliers_count = models.IntegerField(blank=True, null=True)
    outlier_ciphertext_blob = JSONField(blank=True, null=True)
    outlier = JSONField(blank=True, null=True)
    hydration_improvments = JSONField(blank=True, null=True)
    stress_improvments = JSONField(blank=True, null=True)
    sleep_improvments = JSONField(blank=True, null=True)
    flags = JSONField(blank=True, null=True)
    digestion_improvments = JSONField(blank=True, null=True)
    flag_ciphertext_blob = JSONField(blank=True, null=True)
    muscle_ids = JSONField(blank=True, null=True)
    muscle_improvments = JSONField(blank=True, null=True)
    energy_improvments = JSONField(blank=True, null=True)
    utc_unix_datetime = models.DateTimeField(blank=True, null=True)
    vitality_index = models.CharField(max_length=25, blank=True, null=True)
    title = models.CharField(max_length=250, default='')
    download_url = models.CharField(max_length=250, blank=True, null=True)
    channels = JSONField(blank=True, null=True)
    is_favorite = models.BooleanField(default=False)
    brainhealth_category = models.CharField(max_length=250, default='')
    trended_data = JSONField(blank=True, null=True)
    improvement_percent = models.CharField(max_length=250, default='')
    initial_audio_format = models.CharField(max_length=64, default='wav')
    trended_vitality_score = models.CharField(max_length=60, default='')
    covid_buckets = JSONField(default={})
    flag_percentage = JSONField(default={})

    class Meta:
        db_table = 'audios'


class Constants(models.Model):
    audio = models.ForeignKey(
        Audios, related_name='constants', on_delete=models.CASCADE, blank=True, null=True)
    smoothing_factor = models.IntegerField(blank=True, null=True)
    initial_frequency = models.CharField(max_length=30, blank=True, null=True)
    version = models.CharField(max_length=30, blank=True, null=True)
    date = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = 'constants'


class Csvs(models.Model):
    audio = models.ForeignKey(
        Audios, related_name='csvs', on_delete=models.CASCADE, blank=True, null=True)
    base = models.CharField(max_length=30, blank=True, null=True)
    high = models.IntegerField(blank=True, null=True)
    low = models.IntegerField(blank=True, null=True)
    recommendation = models.CharField(max_length=30, blank=True, null=True)
    version = models.CharField(max_length=30, blank=True, null=True)

    class Meta:
        db_table = 'csvs'


class Outliers(models.Model):
    audio = models.ForeignKey(
        Audios, related_name='outliers', on_delete=models.CASCADE, blank=True, null=True)
    decibel = JSONField(blank=True, null=True)
    variance = JSONField(blank=True, null=True)
    high_freq = models.IntegerField(blank=True, null=True)
    low_freq = models.IntegerField(blank=True, null=True)

    class Meta:
        db_table = 'outliers'


class Scores(models.Model):
    audio = models.ForeignKey(
        Audios, related_name='scores', on_delete=models.CASCADE, blank=True, null=True)
    date = models.DateTimeField(blank=True, null=True)
    stress_score = JSONField(blank=True, null=True)
    energy_score = JSONField(blank=True, null=True)
    muscle_score = JSONField(blank=True, null=True)
    sleep_score = JSONField(blank=True, null=True)
    digestion_score = JSONField(blank=True, null=True)
    hydration_score = JSONField(blank=True, null=True)

    class Meta:
        db_table = 'scores'


class Sessions(models.Model):
    user = models.ForeignKey('Users', related_name='sessions',
                             on_delete=models.CASCADE, db_column='user')
    device_type = models.CharField(max_length=100, blank=True, default="")
    device_token = models.CharField(max_length=400, blank=True, default="")
    access_token = models.CharField(max_length=100, blank=True, default="")
    token_is_valid = models.BooleanField(blank=True, default=False)
    is_admin = models.BooleanField(blank=True, default=False)

    class Meta:
        db_table = 'sessions'


class GreetingTitle(models.Model):
    TYPES = (
        ('BETTER', 'improved since last time'),
        ('SAME', 'stayed the same'),
        ('WORSE', 'got worse from previous time'),
    )
    value = models.CharField(max_length=255, blank=True, null=True)
    type = models.CharField(max_length=10, choices=TYPES)

    class Meta:
        db_table = 'greeting_title'


class Group(models.Model):
    name = models.CharField(max_length=255)
    description = models.CharField(max_length=512,  blank=True, null=True)

    def __str__(self):
        return self.name

    class Meta:
        db_table = 'group'


class Users(models.Model):
    attributes = models.ManyToManyField(
        Attribute, blank=True, null=True, related_name="users")
    groups = models.ManyToManyField(
        Group, blank=True, null=True, related_name="users")
    created_by = models.ForeignKey(get_user_model(),
                                   on_delete=models.SET_NULL,
                                   null=True, blank=True)
    configuration_db = models.ForeignKey('configs.ConfigurationSet',
                                         on_delete=models.PROTECT, default=1)
    email = models.CharField(max_length=100, blank=True, null=True)
    first_name = models.CharField(max_length=100, blank=True, null=True)
    photo_urls = ArrayField(
        models.CharField(max_length=512),
        size=100, default=[]
    )
    last_name = models.CharField(max_length=100, blank=True, null=True)
    full_name = models.CharField(max_length=100, blank=True, null=True)
    password = models.CharField(max_length=100, blank=True, null=True)
    registration_type = models.CharField(max_length=100, default="", blank=True)
    facebook_id = models.CharField(max_length=100, default="", blank=True)
    google_id = models.CharField(max_length=100, default="", blank=True)
    apple_id = models.CharField(max_length=100, default="", blank=True)
    login_count = models.IntegerField(default=0, blank=True)
    is_deleted = models.BooleanField(blank=True, null=True, default=False)
    registration_date = models.DateTimeField(auto_now_add=True, null=True)
    security_code = models.CharField(max_length=30, default="", blank=True)
    is_active = models.BooleanField(default=True)
    is_valid_qpc_user = models.BooleanField(null=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)

    TYPES = (
        ('subject', 'subject'),
        ('vitality', 'vitality'),
        ('brain_health', 'brainhealth'),
        ('forte', 'forte'),
        ('qpc_admin', 'qpc_admin'),
        ('qpc_subject', 'qpc_subject'),
        ('checkup_proxy', 'checkup_proxy'),
        ('checkup_consumer', 'checkup_consumer'),
        ('vri_demo_admin', 'vri_demo_admin'),
        ('vri_demo_subject', 'vri_demo_subject'),
        ('vri_one_admin', 'vri_one_admin'),
        ('vri_one_subject', 'vri_one_subject'),
        ('checkup_naked', 'checkup_naked'),
    )
    user_type = models.CharField(max_length=64, choices=TYPES)
    qpc_admin = models.ForeignKey('Users', on_delete=models.CASCADE, null=True, blank=True, related_name='subjects')
    apikey = models.ForeignKey('adminUsers.ApiKeyModel', on_delete=models.SET_NULL, null=True, blank=True, related_name='users')

    def __str__(self):
        return f'{self.first_name} {self.last_name} ({self.email})'

    class Meta:
        db_table = 'users'


class UserProfiles(models.Model):
    sex_choices = (
        ('Male', 'Male'),
        ('Female', 'Female'),
        ('Intersex', 'Intersex'),
        ('Prefer not to say', 'Prefer not to say'),
        ('No Response', 'No Response'),
    )
    user = models.ForeignKey(Users, related_name='profiles',
                             on_delete=models.CASCADE, blank=True, null=True)
    dob = models.CharField(max_length=100, default="", blank=True)
    weight = models.CharField(max_length=100, default="", blank=True)
    height = models.CharField(max_length=100, default="", blank=True)
    sex = models.CharField(max_length=64, choices=sex_choices, default="", blank=True)
    diet_preference = models.TextField(default="", blank=True)
    goals = models.TextField(default="", blank=True)
    exercises = models.TextField(default="", blank=True)
    image = models.CharField(max_length=255, default="", blank=True)

    location_city = models.CharField(max_length=100, default="", blank=True)
    location_country = models.CharField(max_length=100, default="", blank=True)
    location_latitude = models.CharField(max_length=100, default="", blank=True)
    location_longitude = models.CharField(
        max_length=100, default="", blank=True)
    location_postcode = models.CharField(
        db_column='location_postCode', max_length=100, default="", blank=True)
    location_province = models.CharField(max_length=100, default="", blank=True)
    location_state = models.CharField(max_length=100, default="", blank=True)
    location_zipcode = models.CharField(
        db_column='location_zipCode', max_length=100, default="", blank=True)
    timezone = models.CharField(choices=[(tz, tz) for tz in pytz.all_timezones],max_length=100, default='UTC')
    medical_consent_received = models.DateTimeField(null=True, blank=True)
    language_preference = models.ForeignKey(
        Languages, on_delete=models.SET_NULL, null=True, blank=True)
    covid_status = models.ForeignKey(
        'qpc.CovidTestStatus', on_delete=models.SET_NULL, null=True, blank=True)
    qpc_file_data = models.FileField(null=True, storage=QPCFileDataStorage(), blank=True)

    class Meta:
        db_table = 'user_profiles'

    def __str__(self):
        return f'{self.user} profile'


class Reports(models.Model):
    audio_id = models.ForeignKey(Audios, on_delete=models.CASCADE)
    condition_id = models.ForeignKey(
        ConditionSet, on_delete=models.CASCADE, related_name='report_caches')
    download_url = models.CharField(max_length=250, blank=True, null=True)
    created = models.DateField(auto_now_add=True)
    categories = ArrayField(
        models.CharField(max_length=20, blank=True, null=True),
        size=100, blank=True, null=True
    )
    subcategories = ArrayField(
        models.CharField(max_length=20, blank=True, null=True),
        size=100, blank=True, null=True
    )
    order_by_category = models.CharField(max_length=20, blank=True, null=True)
    csv_settings = models.CharField(max_length=20, blank=True, null=True)

    class Meta:
        db_table = 'reports'


class PushNotifications(models.Model):
    DEVICE_TYPES = [
        ('ios', 'Ios'),
        ('android', 'Android')
    ]
    STATUSES = [
        ('sent', 'Sent'),
        ('pending', 'Pending')
    ]
    device_token = models.CharField(max_length=2048)
    device_type = models.CharField(max_length=64, choices=DEVICE_TYPES)
    status = models.CharField(max_length=64, choices=STATUSES, default='pending')
    user = models.ForeignKey(Users, on_delete=models.CASCADE, null=True, related_name='push_notifications')

    def __str__(self):
        return f"{self.user.email} {self.device_type}"

from .signals import * # noqa
