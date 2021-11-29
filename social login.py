from .imports import *


@api_view(http_method_names=['POST'])
@permission_classes([AllowAny])
@psa()
def exchange_facebook_token(request, backend):
    serializer = SocialSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)

    user, data = request.backend.do_auth(serializer.validated_data['access_token'])

    if user:
        try:
            user.avatar_url = data['picture']['data']['url']
        except Exception:
            pass
        is_new = False
        if not user.phone:
            is_new = True

        user_type = Group.objects.get(pk=serializer.validated_data.get('user_type'))
        if user.user_type and user.user_type.pk != user_type.pk:
           raise ValidationError(detail={'detail': 'User with this email is register already'})
        user.save()
        token, _ = Token.objects.get_or_create(user=user)
        return Response({'token': token.key,
                         "is_new": is_new})


@api_view(http_method_names=['POST'])
@permission_classes([AllowAny])
def exchange_google_code(request):
    debug = to_python(request.query_params.get("debug", None))

    client_id = settings.GOOGLE_WEB_APP_CLIENT_ID
    client_secret = settings.GOOGLE_WEB_APP_CLIENT_SECRET

    redirect_uri = f'{settings.FRONTEND_URL}google-auth-callback'

    if debug:
        redirect_uri = 'http://localhost:3000/google-auth-callback'

    header = {"Content-Type": "application/json"}

    serializer = GoogleCodeSerializer(data=request.data)
    serializer.is_valid(raise_exception=True)
    code = serializer.validated_data.get('code')
    access_token = serializer.validated_data.get('access_token')
    if code:
        payload = {"client_id": client_id,
                   "client_secret": client_secret,
                   "redirect_uri": redirect_uri,
                   "grant_type": "authorization_code",
                   "code": code
                   }
        response = requests.post("https://www.googleapis.com//oauth2/v4/token", headers=header,
                                 data=json.dumps(payload))

        if response.status_code != 200:
            return Response({'detail': response.reason}, status=response.status_code)
        data = response.json()
        access_token = data['access_token']

    header = {"Content-Type": "application/json",
              "Authorization": "Bearer " + access_token}

    response = requests.get("https://www.googleapis.com/oauth2/v1/userinfo", headers=header)

    result = response.json()
    email = result['email']
    first_name = result['given_name']
    last_name = result['family_name']
    avatar_url = result['picture']
    user_type = serializer.validated_data.get('user_type')
    is_new = False

    user = User.objects.filter(email=email)
    if user:
        user = user.first()
        if user.user_type.pk != user_type:
            raise ValidationError(detail={'detail': 'User with this email is register already'})
    else:
        user = User.objects.create_user(email=email, first_name=first_name, last_name=last_name, avatar_url=avatar_url,
                                        user_type=user_type)
    if not user.phone:
        is_new = True
    token, _ = Token.objects.get_or_create(user=user)
    return Response({'token': token.key,
                     'is_new': is_new}, status=status.HTTP_200_OK)


@api_view(http_method_names=['GET'])
@permission_classes([AllowAny])
def generate_url(request):
    debug = to_python(request.query_params.get("debug", None))

    client_id = settings.GOOGLE_WEB_APP_CLIENT_ID

    redirect_uri = f'{settings.FRONTEND_URL}google-auth-callback'
    if debug:
        redirect_uri = 'http://localhost:3000/google-auth-callback'

    params = {"redirect_uri": redirect_uri,
              "response_type": "code",
              "client_id": client_id,
              "scope": "openid%20email%20profile"
              }

    params = '&'.join(["{}={}".format(k, v) for k, v in params.items()])
    url = "https://accounts.google.com/o/oauth2/v2/auth?" + params
    return Response({'url': url})

