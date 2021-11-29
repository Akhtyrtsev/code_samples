@api_view(http_method_names=['POST', 'GET'])
@permission_classes([AllowAny])
def redirect_call(request):

    caller = request.POST.get('From')[2:]
    twillio_phone = request.POST.get('To')[2:]
    logger.info("Caller: %s calling %s.", str(caller), str(twillio_phone))

    appointment = Appointment.objects.filter(phone__number=twillio_phone).first()
    if appointment:
        logger.info("Appointment Found: %s. Client: %s (%s) Valet: %s (%s)", str(appointment), str(appointment.client), str(appointment.client.phone), str(appointment.valet), str(appointment.valet.phone))
    else:
        site = Site.objects.get(pk=1)
        path = f"https://{site.domain}{reverse('twilio-support')}"
        logger.warning("Appointment Not Found: %s Phone: %s ", str(appointment), str(twillio_phone))
        response = VoiceResponse()
        gather = Gather(action=path, method='GET')
        gather.say('We can`t find an appointment for this number. '
                   'Please, press 0 to connect to support')
        response.append(gather)
        response.say('We didn\'t receive any input. Goodbye!')
        return HttpResponse(str(response), content_type="application/xml")

    phone = None
    if caller == appointment.client.phone:
        phone = appointment.valet.phone
    elif caller == appointment.valet.phone:
        phone = appointment.client.phone
    phone = '+1' + phone

    logger.info("Connecting %s to %s from %s", str(caller), str(phone), str(twillio_phone))

    response = VoiceResponse()
    logger.info('Response: %s', str(response))

    response.dial(phone, caller_id=twillio_phone)

    return HttpResponse(str(response), content_type="application/xml")


@api_view(http_method_names=['GET', 'POST', 'PUT', 'OPTIONS', 'PATCH'])
@permission_classes([AllowAny])
def support_call_xml_view(request):
    phone = os.environ.get("SUPPORT_PHONE")
    response = VoiceResponse()
    response.dial(phone)
    print(phone)
    return HttpResponse(str(response), content_type="application/xml")
