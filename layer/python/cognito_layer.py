import boto3
import os
import json
import base64
import jwt
from jwt.algorithms import RSAAlgorithm
import requests as req

#logger オブジェクト
from logger_layer import ApplicationLogger
logger = ApplicationLogger(name=__name__, env_info="dev") if os.getenv("AWS_SAM_LOCAL") else ApplicationLogger(name=__name__)


class CognitoObject():
    def __init__(self):
        pass
    @classmethod
    def get_user_info_from_id_token(self, id_token):
        logger.debug("token received. token: {}".format(id_token))
        #　環境変数取得
        issuer = os.getenv('COGNITO_ISSUER')
        audience = os.getenv('COGNITO_AUDIENCE')
        try:
            # header情報取得
            header = jwt.get_unverified_header(id_token)
            # 公開鍵取得
            logger.debug("Requesting public crt")
            url = "https://cognito-idp.ap-northeast-1.amazonaws.com/ap-northeast-1_pClKUpMPC/.well-known/jwks.json"
            public_key_res = req.get(url)
            public_key_res.raise_for_status()
            logger.debug("Success requesting public cert")
            public_key = public_key_res.json()
            logger.debug("public key: {}".format(public_key))
            jwk_list = public_key["keys"]
            # header情報のkidと一致する鍵を取得
            jwk_key = next(filter(lambda k: k['kid'] == header['kid'], jwk_list))
            # 公開鍵へデコード
            logger.debug(json.dumps(jwk_key, indent=2))
            public_key = RSAAlgorithm.from_jwk(json.dumps(jwk_key))
            # 署名の検証 & tokenデコード
            claims = jwt.decode(
                id_token,
                public_key,
                issuer=issuer,
                audience=audience,
                algorithms=["RS256"]
            )
            logger.debug("claims: {}".format(claims))
            return claims
        except req.exceptions.HTTPError as e:
            logger.warn(e)
            logger.warn("Failed requesting public cert")
        except Exception as e:
            logger.warn(e)
            logger.warn("Failed decoding token.")
            raise
