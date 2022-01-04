<?php
/**
 * @var \App\View\AppView $this
 * @var \App\Model\Entity\DocExtratosFundo $docExtratosFundo
 */
?>
<div class="row">
    <aside class="column">
        <div class="side-nav">
            <h4 class="heading"><?= __('Actions') ?></h4>
            <?= $this->Form->postLink(
                __('Delete'),
                ['action' => 'delete', $docExtratosFundo->cnpj_fundo_id],
                ['confirm' => __('Are you sure you want to delete # {0}?', $docExtratosFundo->cnpj_fundo_id), 'class' => 'side-nav-item']
            ) ?>
            <?= $this->Html->link(__('List Doc Extratos Fundos'), ['action' => 'index'], ['class' => 'side-nav-item']) ?>
        </div>
    </aside>
    <div class="column-responsive column-80">
        <div class="docExtratosFundos form content">
            <?= $this->Form->create($docExtratosFundo) ?>
            <fieldset>
                <legend><?= __('Edit Doc Extratos Fundo') ?></legend>
                <?php
                    echo $this->Form->control('tipo_anbima_classe_id', ['options' => $tipoAnbimaClasses]);
                    echo $this->Form->control('APLIC_MIN');
                    echo $this->Form->control('QT_DIA_PAGTO_COTA');
                    echo $this->Form->control('QT_DIA_PAGTO_RESGATE');
                    echo $this->Form->control('PR_COTA_ETF_MAX');
                ?>
            </fieldset>
            <?= $this->Form->button(__('Submit')) ?>
            <?= $this->Form->end() ?>
        </div>
    </div>
</div>
