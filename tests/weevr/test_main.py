from weevr import main


def test_main_prints_hello(capsys) -> None:
    main()
    captured = capsys.readouterr()
    assert captured.out.strip() == "Hello from weevr!"
