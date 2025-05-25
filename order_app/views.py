import logging
import os
import tempfile

from django.conf import settings
from django.core.files.base import ContentFile
from django.core.files.storage import default_storage
from django.http import Http404, HttpResponse, JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt

from order_processor import OrderProcessor

logger = logging.getLogger(__name__)


def index(request):
    """Main page with file upload form"""
    return render(request, "order_app/index.html")


@csrf_exempt
def process_files(request):
    """Process uploaded files and return download link"""
    if request.method != "POST":
        return JsonResponse({"error": "POST method required"}, status=405)

    # Check if files are uploaded
    master_file = request.FILES.get("master_file")
    order_file = request.FILES.get("order_file")

    if not master_file or not order_file:
        return JsonResponse({"error": "두 파일 모두 업로드해주세요."}, status=400)

    # Check file extensions
    if not (master_file.name.endswith(".xlsx") and order_file.name.endswith(".xlsx")):
        return JsonResponse(
            {"error": "Excel 파일(.xlsx)만 업로드 가능합니다."}, status=400
        )

    try:
        # Create temporary directory for processing
        with tempfile.TemporaryDirectory() as temp_dir:
            # Save uploaded files temporarily
            master_path = os.path.join(temp_dir, "master.xlsx")
            order_path = os.path.join(temp_dir, "order.xlsx")

            with open(master_path, "wb") as f:
                for chunk in master_file.chunks():
                    f.write(chunk)

            with open(order_path, "wb") as f:
                for chunk in order_file.chunks():
                    f.write(chunk)

            # Process files using OrderProcessor
            processor = OrderProcessor(order_path, master_path)
            result_df = processor.process_all()

            # Save result to media directory
            output_filename = "결과물.csv"
            media_path = os.path.join(settings.MEDIA_ROOT, output_filename)

            # Ensure media directory exists
            os.makedirs(settings.MEDIA_ROOT, exist_ok=True)

            # Save processed data
            result_df.to_csv(media_path, index=False, encoding="utf-8-sig")

            return JsonResponse(
                {
                    "success": True,
                    "message": f"처리 완료! {len(result_df)}개 행이 처리되었습니다.",
                    "download_url": f"/download/{output_filename}/",
                    "rows_processed": len(result_df),
                }
            )

    except Exception as e:
        logger.error(f"Error processing files: {e}")
        return JsonResponse(
            {"error": f"파일 처리 중 오류가 발생했습니다: {str(e)}"}, status=500
        )


def download_file(request, filename):
    """Download processed file"""
    file_path = os.path.join(settings.MEDIA_ROOT, filename)

    if not os.path.exists(file_path):
        raise Http404("파일을 찾을 수 없습니다.")

    with open(file_path, "rb") as f:
        response = HttpResponse(f.read(), content_type="text/csv")
        response["Content-Disposition"] = f'attachment; filename="{filename}"'
        return response
