<?php
declare(strict_types=1);

namespace App\Model\Entity;

use Cake\ORM\Entity;

/**
 * CadastroFundo Entity
 *
 * @property int $cnpj_fundo_id
 * @property \Cake\I18n\FrozenDate $DT_CONST
 * @property int $tipo_classe_fundo_id
 * @property \Cake\I18n\FrozenDate $DT_INI_CLASSE
 * @property int $tipo_rentabilidade_fundo_id
 * @property bool $CONDOM_ABERTO
 * @property bool $FUNDO_COTAS
 * @property bool $FUNDO_EXCLUSIVO
 * @property bool $INVEST_QUALIF
 * @property int $gestor_fundo_id
 * @property int $administrador_fundo_id
 * @property \Cake\I18n\FrozenDate $DT_REG_CVM
 *
 * @property \App\Model\Entity\CnpjFundo $cnpj_fundo
 * @property \App\Model\Entity\TipoClasseFundo $tipo_classe_fundo
 * @property \App\Model\Entity\TipoRentabilidadeFundo $tipo_rentabilidade_fundo
 * @property \App\Model\Entity\GestorFundo $gestor_fundo
 * @property \App\Model\Entity\AdministradorFundo $administrador_fundo
 */
class CadastroFundo extends Entity
{
    /**
     * Fields that can be mass assigned using newEntity() or patchEntity().
     *
     * Note that when '*' is set to true, this allows all unspecified fields to
     * be mass assigned. For security purposes, it is advised to set '*' to false
     * (or remove it), and explicitly make individual fields accessible as needed.
     *
     * @var array
     */
    protected $_accessible = [
        'DT_CONST' => true,
        'DT_REG_CVM' => true,
        'cnpj_fundo' => true,
        'tipo_classe_fundo' => true,
        'tipo_rentabilidade_fundo' => true,
        'gestor_fundo' => true,
        'administrador_fundo' => true,
    ];
}
