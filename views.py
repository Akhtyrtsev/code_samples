from rest_framework import viewsets, mixins

from .helper.confirm_subscription_service import generate_payment_method_link, send_mail
from .helper.fee_helper import apply_fee_helper
from .models import Location, Tenant, Payments, Unit, AllLocationReviews, LateFee, BrivoGroup
from .serializers import LocationSerializer, TenantSerializer, OneLocationSerializer, BookingSerializer, \
    AllLocationReviewsSerializer, LocationsSerializer, BookingIdSerializer, BookingUnitIdSerializer
from .models import Location, Tenant, WaitList, Booking, TenantSubscription, Subscription, PromoCode, TenantCharge
from .serializers import LocationSerializer, TenantSerializer, OneLocationSerializer, WaitListSerializer,\
    ListPaymentsSerializer, CreatePaymentsSerializer, TenantSubscriptionSerializer, UnitCategoryPriceSerializer, \
    CheckPromoDodeSerializer
from rest_framework.response import Response
from .helper.destination_helper import get_distance_by_address
from .helper.docusign_signing import embedded_signing, update_token
from .helper.subscription_helper import make_subscription_charge, calculate_discount_sum
from .helper.refund_helper import make_refund
from django.conf import settings
from rest_framework.exceptions import NotFound, ValidationError
from rest_framework import status
from django.http import JsonResponse
from django.shortcuts import redirect, reverse
from datetime import datetime, timedelta, date
from docusign_esign import ApiClient, EnvelopesApi
from rest_framework import filters
from django.utils.timezone import now
from django.views.decorators.csrf import csrf_exempt
from django.template.response import TemplateResponse
from django.views.decorators.clickjacking import xframe_options_exempt
from apies.brivo_api import BrivoApi
from dateutil.parser import parse
from django.http import HttpResponseRedirect
import requests
import base64
from django.contrib.sites.models import Site
from django.contrib import messages
import os
import pickle
import json
import stripe

api_client = ApiClient(oauth_host_name=settings.DOCUSIGN_OAUTH_HOST_NAME)
stripe.api_key = settings.STRIPE_SECRET_KEY
base64_brivo_credentials = b"Basic " + base64.b64encode(
    str.encode("{}:{}".format(settings.BRIVO_CLIENT_ID, settings.BRIVO_CLIENT_SECRET)))


class LocationView(viewsets.GenericViewSet, mixins.ListModelMixin, mixins.RetrieveModelMixin):
    queryset = Location.objects.all()\
        .prefetch_related('photos')\
        .prefetch_related('unit_categories')\
        .prefetch_related('unit_categories__units')
    serializer_class = LocationSerializer
    location_limit = 5
    threshold = 1.75  # miles

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        serializer = OneLocationSerializer(instance)
        return Response(serializer.data)

    def list(self, request, *args, **kwargs):
        queryset = self.filter_queryset(self.get_queryset())
        page = self.paginate_queryset(queryset)
        if page is not None:
            serializer = self.get_serializer(page, many=True)
            return self.get_paginated_response(serializer.data)
        search = request.query_params.get('search')
        if not search:
            serializer = self.get_serializer([], many=True)
            return Response(serializer.data)
        queryset = get_distance_by_address(queryset, search, self.location_limit, self.threshold)
        serializer = self.get_serializer(queryset, many=True)
        return Response(serializer.data)


class TenantView(viewsets.GenericViewSet, mixins.CreateModelMixin):
    queryset = Tenant.objects.all()
    serializer_class = TenantSerializer


class PaymentsViewSet(viewsets.ModelViewSet):
    queryset = Payments.objects.all()
    serializer_class = ListPaymentsSerializer

    def get_queryset(self):
        user = self.request.query_params.get('tenant_id')
        user = Tenant.objects.get(pk=user)
        return user.payments.all()

    def get_serializer_class(self):
        if self.action == 'create':
            return CreatePaymentsSerializer
        return ListPaymentsSerializer

    def create(self, request, *args, **kwargs):
        user = self.request.query_params.get('tenant_id')
        try:
            user = Tenant.objects.get(pk=user)
        except Exception:
            raise NotFound({'detail': 'User not found'})
        serializer = self.get_serializer_class()(data=request.data)
        serializer.is_valid(raise_exception=True)
        token = serializer.validated_data.get('token')
        if not token:
            raise ValidationError(detail={'detail': "Token is not provided"})

        try:
            customer = stripe.Customer.retrieve(user.stripe_id)
        except Exception:
            try:
                customer = stripe.Customer.create(
                    email=user.email
                )
            except stripe.error.StripeError as e:
                raise ValidationError(detail={'detail': e.error.message})
        user.stripe_id = customer.id
        user.save()
        try:
            token = stripe.Token.retrieve(token)
        except stripe.error.StripeError as e:
            raise ValidationError(detail={'detail': e.error.message})
        try:
            card_print = token['card']['fingerprint']
            p = Payments.objects.filter(fingerprint=card_print, tenant=user)
        except Exception as e:
            bank_print = token['bank_account']['fingerprint']
            p = Payments.objects.filter(tenant=user, fingerprint=bank_print)

        try:
            source = stripe.Customer.create_source(
                user.stripe_id,
                source=token
            )
            source_id = source['id']
            stripe.Customer.modify(
                user.stripe_id,
                default_source=source_id
            )
            bank_account = stripe.Customer.retrieve_source(
                user.stripe_id,
                source_id
            )
            try:
                bank_account.verify(amounts=[32, 45])
            except Exception as e:
                print(e)
        except stripe.error.StripeError as e:
            if e.error.decline_code == 'do_not_honor':
                raise ValidationError(detail={'detail': 'Card declined. '
                                                        'Please contact your card issuer for more information.'})
            raise ValidationError(detail={'detail': e.error.message})
        fingerprint = source.get('fingerprint', None)
        card_type = source.get('brand', None)
        last_4 = source.get('last4', None)
        card_id = source.get('id', None)
        exp_month = source.get('exp_month', None)
        exp_year = source.get('exp_year', None)
        type = source.get('type', None)
        try:
            exp_date = date(year=exp_year, month=exp_month, day=1)
        except Exception:
            exp_date = None
        p = Payments.objects.create(tenant=user, stripe_id=card_id, last_4=last_4, card_type=card_type,
                                    expire_date=exp_date, fingerprint=fingerprint, type=type)

        serializer = ListPaymentsSerializer(p, many=False)
        return Response(serializer.data, status=status.HTTP_201_CREATED)


class WaitListCreate(viewsets.GenericViewSet, mixins.CreateModelMixin):
    queryset = WaitList.objects.all()
    serializer_class = WaitListSerializer


class BookingCreate(viewsets.GenericViewSet, mixins.CreateModelMixin):
    queryset = Booking.objects.all()
    serializer_class = BookingSerializer


def docusign_oauth(request):

    api_client.get_oauth_host_name()
    url = api_client.get_authorization_uri(
        client_id=settings.DOCUSIGN_CLIENT_ID,
        redirect_uri=request.build_absolute_uri(reverse(settings.DOCUSIGN_CREDS_ENDPOINT)),
        scopes=["signature", "impersonation"],
        response_type='code'
    )

    return redirect(url)


def docusign_callback(request):

    code = request.GET.get('code')
    token_obj = api_client.generate_access_token(settings.DOCUSIGN_CLIENT_ID, settings.DOCUSIGN_CLIENT_SECRET, code)
    token_obj.expiration_date = datetime.utcnow() + timedelta(seconds=int(token_obj.expires_in))
    with open(settings.DOCUSIGN_API_CREDENTIAL, 'wb') as token:
        pickle.dump(token_obj, token)

    return redirect(reverse('admin:index'))


def generate_docusign_link(request):
    tenant_id = request.GET.get('tenant')
    booking_id = request.GET.get('booking')
    redirect_url = request.GET.get('redirect_url', 'http://localhost:8000/')
    if not all([tenant_id, booking_id, redirect_url]):
        return JsonResponse({'error': 'tenant_id/booking_id/redirect_url not specified.'}, status=400)
    tenant = Tenant.objects.filter(id=tenant_id).first()
    booking = Booking.objects.filter(id=booking_id).first()
    if not tenant or not booking:
        return JsonResponse({'error': 'Tenant or booking is not exist.'}, status=400)
    context = embedded_signing(request, tenant, booking, redirect_url)
    return JsonResponse(context, status=200)


def save_docusign_document(request):
    account_id = request.GET.get('account_id')
    envelope_id = request.GET.get('envelope_id')
    booking_id = request.GET.get('booking_id')
    event = request.GET.get('event')
    html = 'docusign/success.html'
    if event == 'signing_complete':
        if os.path.exists(settings.DOCUSIGN_API_CREDENTIAL):
            with open(settings.DOCUSIGN_API_CREDENTIAL, 'rb') as token:
                token_obj = pickle.load(token)
                if datetime.utcnow() >= token_obj.expiration_date:
                    token_obj = update_token(token_obj)
        api = ApiClient()
        base_path = settings.DOCUSIGN_BASE_API_PATH + 'restapi'
        api.host = base_path
        api.set_default_header("Authorization", "Bearer " + token_obj.access_token)

        envelope_api = EnvelopesApi(api)
        r = envelope_api.get_document(account_id, 1, envelope_id, show_changes=True)
        name = r.split('/')[2]
        import subprocess
        lease_filename = 'Lease_' + now().isoformat(sep='_', timespec='milliseconds') + '.pdf'
        subprocess.call(["mv", r, "/code/media/docusign/"])
        subprocess.call(["mv", "/code/media/docusign/" + name, "/code/media/docusign/" + lease_filename])
        booking = Booking.objects.filter(id=booking_id).first()
        if not booking:
            print('Booking was removed')
            return TemplateResponse(request, html, {'event': event})
        booking.sign_document = f'/docusign/{lease_filename}'
        booking.save()

    return TemplateResponse(request, html, {'event': event})


class TenantSubscriptionViewSet(viewsets.ModelViewSet):
    queryset = TenantSubscription.objects.all()
    serializer_class = TenantSubscriptionSerializer


class UnitCategoryPriceViewSet(viewsets.ModelViewSet):
    queryset = Subscription.objects.all()
    serializer_class = UnitCategoryPriceSerializer

    def get_queryset(self):
        unit = self.request.query_params.get('unit')
        if unit:
            return Subscription.objects.filter(unit__pk=unit)
        return self.queryset


@csrf_exempt
def complete_booking(request):
    tenant_id = request.POST.get('tenant_id')
    subscription_id = request.POST.get('subscription_id')
    booking_id = request.POST.get('booking_id')
    promocode_id = request.POST.get('promocode_id')
    payment_id = request.POST.get('payment_id')
    repay = request.POST.get('repay')
    if not all([tenant_id, subscription_id, booking_id, payment_id]):
        return JsonResponse({'error': 'tenant_id/subscription_id/booking_id/payment_id not specified.'}, status=400)
    booking = Booking.objects.filter(id=booking_id).first()
    if booking.unit.locked_by:
        return JsonResponse({'error': 'This unit is locked already.'}, status=400)
    time = datetime.utcnow()
    Unit.objects.filter(id=booking.unit.id).update(locked_by=booking.id, date_locked=time)

    subscription = Subscription.objects.filter(id=subscription_id).first()
    tenant = Tenant.objects.filter(id=tenant_id).first()
    payment = Payments.objects.filter(id=payment_id).first()
    if not all([booking, subscription, tenant]):
        return JsonResponse({'error': 'tenant/subscription/booking/ does not exist.'}, status=400)

    if booking.unit.status != 'available':
        return JsonResponse({'error': 'This unit is booked already.'}, status=400)
    promocode = None
    if promocode_id:
        promocode = PromoCode.objects.filter(id=promocode_id, is_active=True).first()
        if not promocode:
            return JsonResponse({'error': 'Promocode does not exist.'}, status=400)
        
        booking.promo_code = promocode
    try:
        make_subscription_charge(tenant, subscription, booking, promocode=promocode, payment=payment, repay=repay)
    except Exception as e:
        print(e)
        return JsonResponse({'error': 'Payment failed'}, status=400)
    return JsonResponse({'success': 'Booking completed successfully.'}, status=200)


class CheckPromoCodeViewSet(viewsets.ModelViewSet):
    queryset = PromoCode.objects.all()
    serializer_class = CheckPromoDodeSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer_class()(data=request.data)
        serializer.is_valid(raise_exception=True)
        code = serializer.validated_data.get('code')
        if not code:
            raise ValidationError(detail={'detail':'"code" is required'})
        subscription = request.data.get('subscription')
        if not subscription:
            raise ValidationError(detail={'detail': '"subscription" is required'})
        subscription = Subscription.objects.filter(pk=subscription).first()
        if not subscription:
            raise NotFound(detail={'detail':'Subscription doesn`t exist'})
        booking = request.data.get('booking')
        if not booking:
            raise ValidationError(detail={'detail': '"booking" is required'})
        booking = Booking.objects.filter(pk=booking).first()
        if not booking:
            raise NotFound(detail={'detail': 'Booking doesn`t exist'})
        code_instance = PromoCode.objects.filter(code=code, is_active=True).first()
        if code_instance:
            data = serializer.data
            data['id'] = code_instance.id
            data['discount'] = round(calculate_discount_sum(code_instance, subscription, booking.move_in_date) / 100, 2)
            data['promocode_type'] = code_instance.promocode_type
            return Response(data, status=status.HTTP_200_OK)
        raise ValidationError(detail={'detail': 'Provided code is not valid'})


class ValidateSubscription(viewsets.GenericViewSet, mixins.CreateModelMixin):
    queryset = Booking.objects.all()
    serializer_class = BookingIdSerializer

    def create(self, request, *args, **kwargs):
        serializer = self.get_serializer_class()(data=request.data)
        serializer.is_valid(raise_exception=True)
        id = serializer.validated_data.get('booking_id')
        booking = Booking.objects.get(id=id)
        if not booking.subscription:
            raise ValidationError(detail={'detail': 'Subscription is not valid'})
        if booking.subscription.is_payed:
            raise ValidationError(detail={'detail': 'Subscription has been already paid'})
        return Response({'status': 'ok'}, status=status.HTTP_200_OK)


class ValidateUnit(viewsets.GenericViewSet, mixins.CreateModelMixin, mixins.RetrieveModelMixin):
    queryset = Unit.objects.all()
    serializer_class = BookingUnitIdSerializer

    def create(self, request, *args, **kwargs):
        now = datetime.utcnow()
        serializer = self.get_serializer_class()(data=request.data)
        serializer.is_valid(raise_exception=True)
        unit_id = serializer.validated_data.get('unit_id')
        booking_id = serializer.validated_data.get('booking_id')
        unit = Unit.objects.filter(id=unit_id)
        unit.update(locked_by=booking_id, date_locked=now)
        return Response({'status': 'created'}, status=status.HTTP_201_CREATED)

    def retrieve(self, request, *args, **kwargs):
        instance = self.get_object()
        if instance.locked_by:
            raise ValidationError(detail={'detail': 'Unit is locked'})
        return Response({'status': 'ok'}, status=status.HTTP_200_OK)


class Reviews(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = AllLocationReviews.objects.all()
    serializer_class = AllLocationReviewsSerializer


class GetLocationsAjax(viewsets.GenericViewSet, mixins.ListModelMixin):
    queryset = Location.objects.all()
    serializer_class = LocationsSerializer


def brivo_login(request):
    login_url = 'https://auth.brivo.com/oauth/authorize?response_type=code&client_id=' + settings.BRIVO_CLIENT_ID
    response = HttpResponseRedirect(redirect_to=login_url)
    response['Authorization'] = base64_brivo_credentials
    response['api-key'] = settings.BRIVO_API_KEY
    return response


def brivo_call_back(request):
    import ast
    now = datetime.utcnow().timestamp()
    code = request.GET.get('code')
    retrieve_token_url = f'https://auth.brivo.com/oauth/token?grant_type=authorization_code&code={code}'
    response = requests.post(retrieve_token_url, headers={'Authorization': base64_brivo_credentials, 'api-key': settings.BRIVO_API_KEY})
    token = ast.literal_eval(response.content.decode())
    BrivoApi(settings.BRIVO_API_CREDENTIAL).save_token(token, now)
    return redirect(reverse('admin:index'))


def make_refund_view(request):
    booking_id = request.GET.get('booking_id')
    if booking_id:
        booking = Booking.objects.filter(id=booking_id).first()
        if not booking.subscription or not booking.subscription.stripe_id:
            messages.warning(request, 'Subscription not paid')
            return JsonResponse({'status': 'failed'})
        if booking:
            try:
                refund = make_refund(booking)
                redirect_url = f'http://{Site.objects.get_current().domain}' + f'/admin/portal/refund/{refund.id}/change/'
                return JsonResponse({'status': 'success', 'redirect_url': redirect_url})
            except Exception as e:
                print(e)
                messages.warning(request, 'Something went wrong')
    return JsonResponse({'status': 'failed'})


def apply_fee_view(request):
    booking_id = request.GET.get('booking_id')
    fee = LateFee.objects.first()
    if fee and booking_id:
        booking = Booking.objects.filter(id=booking_id).first()
        if booking:
            charge = apply_fee_helper(booking)
            if charge.get('status') != 'succeeded':
                messages.warning(request, 'Something went wrong')
            else:
                messages.success(request, 'Late has been applied')

    return JsonResponse({})


def send_email_view(request):
    booking_id = request.GET.get('booking_id')
    booking = Booking.objects.filter(id=booking_id).first()
    if booking:
        if booking.subscription and not booking.subscription.is_payed:
            url = generate_payment_method_link(booking)
            send_mail(
                context={'confirm_subscription_url': url,
                         'tenant': booking.tenant,
                         'booking': booking,
                         'frontend_url': settings.FRONTEND_URL
                         }, template='email/payment_form.html', title='Local Locker Payment Link',
                to=[booking.tenant.email]
            )
            messages.success(request, 'Email has been sent.')
        return JsonResponse({'status': 'OK'})
    messages.warning(request, 'Something went wrong.')
    return JsonResponse({'status': 'Error'})


def send_change_card_email_view(request):
    booking_id = request.GET.get('booking_id')
    booking = Booking.objects.filter(id=booking_id).first()
    if booking:
        url = generate_payment_method_link(booking, change_card=True)
        send_mail(context={'booking': booking, 'frontend_url': settings.FRONTEND_URL, 'pay_link': url},
                  template='email/card_ready_expire.html', title='Local Locker Card Expiring',
                  to=[booking.tenant.email])
        messages.success(request, 'Email has been sent.')
        return JsonResponse({'status': 'OK'})
    messages.warning(request, 'Something went wrong.')
    return JsonResponse({'status': 'Error'})


@csrf_exempt
def calendly_hook(request):
    brivo = BrivoApi(settings.BRIVO_API_CREDENTIAL)
    phone_field_name = 'phone number'

    response = json.loads(request.body)
    payload = response.get('payload', {})
    event = payload.get('event', {})
    event_type = payload.get('event_type', {})
    invitee = payload.get('invitee', {})
    questions = payload.get('questions_and_answers', {})
    start_time = event.get('start_time')

    event_name = event_type.get('name')
    user_email = invitee.get('email')
    user_name = invitee.get('name')
    phone = [q['answer'] for q in questions if q['question'].lower() == phone_field_name][0]
    first_name = invitee.get('first_name')
    last_name = invitee.get('last_name')

    location = Location.objects.filter(title__icontains=event_name.strip()).first()
    if not location:
        print(f'Location {event_name} not found')
        return
    end_time = parse(start_time) + timedelta(days=1)
    end_time = end_time.strftime('%Y-%m-%dT%H:%M:%SZ')
    data = {
        'firstName': first_name if first_name else user_name,
        'lastName': last_name if last_name else user_name,
        'emails': [{"address": user_email, "type": 'home'}],
        'phoneNumbers': [{"number": phone, "type": 'home'}],
        'effectiveFrom': start_time,
        'effectiveTo': end_time
    }
    tenant = brivo.create_user(data)
    print(tenant)
    if not tenant.get('id'):
        return

    print('Tenant Created')
    if location.brivo_group and location.brivo_group.brivo_id:
        brivo.assign_user_to_group(tenant['id'], location.brivo_group.brivo_id)
    start_time = parse(start_time)
    send_mail(
        context={'location': location, 'tour_date': start_time.strftime("%B %d, %Y at %I:%M %p"), 'pin': tenant['pin'],
                 'pin_effective_date': start_time.strftime("%B %d, %Y"), 'name': first_name if first_name else user_name},
        template='email/calendly_welcome.html', title='Local Locker Tour Confirmation',
        to=[user_email]
    )

    return JsonResponse({'status': 'ok'})
