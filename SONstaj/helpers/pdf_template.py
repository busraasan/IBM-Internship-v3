from fpdf import FPDF

WIDTH = 210
HEIGHT = 297

class PDF(FPDF):
    def header(self):
        self.set_font('Arial', 'B', 16)
        if self.page_no() == 1:
            self.image("./letterheadpink.png",0,0,210)
            self.image("./ibmlogo.png", 170, 20, 25)
        else:
            self.image("./ibmlogo.png", 183, 20, 15)
        self.set_line_width(1)
        self.ln(10)

    def add_small_title(self, text):
        global cursor_y

        if cursor_y+100>210:
            cursor_y = 30
            self.add_page()
        
        self.set_y(cursor_y)
        self.set_font("Arial", size=10)
        self.ln(10)
        self.cell(w=0, h=6, txt=text, ln=4, align='L')

        cursor_y+=20

    def add_titre(self, name, day):
        global cursor_y
        self.set_font("Arial", size = 24)
        self.ln(35)
        self.cell(w=0, txt = name+ " CPU Report", ln = 5, align = 'L')
        self.set_font("Arial", size = 15)
        self.ln(10)
        self.cell(w=0, txt = f'{day}', ln = 4, align = 'L')

        cursor_y = 67.75
        # 24pt + 35mm + 5mm + 15pt + 10mm + 4mm =
        # 39pt + 54mm
        # 67.7584 from top

    def footer(self):
        self.image("./footerpink.png", 0, 285, WIDTH)
        self.set_y(-8)
        self.set_font("Arial", size = 16)
        self.cell(w= 0, h=0, txt = str(self.page_no()) + '        ', ln = 3, align = 'R')

    def add_two_figures(self, figure1, figure2):
        global cursor_y
        # assuming fig sizes are 640x480
        fig_w = 640
        fig_h = 480

        new_w = 100 # width/2 -5
        new_h = (fig_h/fig_w)*new_w

        if cursor_y + new_h > HEIGHT:
            self.add_page()
            cursor_y = 30

        self.image(figure1, 5, cursor_y, new_w)
        self.image(figure2, 5+new_w, cursor_y, new_w)

        cursor_y += new_h

        if cursor_y > 290: # if out of bounds
            cursor_y = 30
            self.add_page()
    
    def add_single_figure(self, figure):
        global cursor_y
        # assuming fig size for single figure is 1000x350
        fig_w = 1000
        fig_h = 350
        new_w = 210 # WIDTH
        new_h = (fig_h/fig_w)*new_w
        if cursor_y + new_h > HEIGHT:
            self.add_page()
            cursor_y = 30
        self.image(figure, 0, cursor_y, new_w)
        cursor_y += new_h
        if cursor_y > 290:
            cursor_y = 30
            self.add_page()

    def print_chapter(self, num, title, name):
        self.add_page()
        self.chapter_title(num, title)
        self.chapter_body(name)

    def add_info(self, text, info):
        global cursor_y
        self.set_y(cursor_y)
        self.set_font("Arial", size = 20)
        self.ln(15)
        self.cell(w=0, txt = text, ln = 2, align = 'C')
        self.set_text_color(117,117,117) 
        self.ln(15)
        self.set_font("Arial", size = 24)
        self.cell(w=0, txt = info, ln = 2, align = 'C')
        self.set_text_color(0,0,0) 
        # 15pt + 14 mm
        # 5.2917 + 14
        # 19.2917
        cursor_y += 4
        if cursor_y > 290:
            cursor_y = 30
            self.add_page()