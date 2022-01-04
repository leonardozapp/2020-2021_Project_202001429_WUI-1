<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\DocInfDiarioFundo $docInfDiarioFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Html->link(__('List Doc Inf Diario Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="docInfDiarioFundos form content">
            <?= $this->Form->create($docInfDiarioFundo) ?>
            <fieldset>
                <legend><?= __('Add Doc Inf Diario Fundo') ?></legend>
                <?php
                    echo $this->Form->control('VL_TOTAL');
                    echo $this->Form->control('VL_QUOTA');
                    echo $this->Form->control('VL_PATRIM_LIQ');
                    echo $this->Form->control('CAPTC_DIA');
                    echo $this->Form->control('RESG_DIA');
                    echo $this->Form->control('NR_COTST');
                    echo $this->Form->control('rentab_diaria');
                    echo $this->Form->control('volat_diaria');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
