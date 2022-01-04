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
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $docInfDiarioFundo->cnpj_fundo_id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $docInfDiarioFundo->cnpj_fundo_id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Doc Inf Diario Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="docInfDiarioFundos form content">
            <?= $this->Form->create($docInfDiarioFundo) ?>
            <fieldset>
                <legend><?= __('Edit Doc Inf Diario Fundo') ?></legend>
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
